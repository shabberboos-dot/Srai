import os, json, time, sqlite3, asyncio, logging, hashlib, csv, io, platform, math, tempfile
from datetime import datetime, timezone, timedelta
from collections import Counter
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TelegramError
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# XHAWON SELF-LEARNING PATTERN AI v8.0 — RESEARCH CONTROL CENTER
# Research/paper testing only. Future outcomes are not guaranteed.
BOT_TOKEN = os.getenv("BOT_TOKEN", "8932852728:AAHvJeyMl-dtO2IJ1eRx2xeRSRSVRLzzySU").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "8746661403").strip()
API_URL = os.getenv("WINGO_API_URL", "https://draw.ar-lottery01.com/WinGo/WinGo_30S/GetHistoryIssuePage.json").strip()
DB_PATH = os.getenv("XHAWON_DB", "xhawon_v6.db")
POLL_SECONDS = max(3, int(os.getenv("POLL_SECONDS", "4")))
LEARN_SECONDS = max(10, int(os.getenv("LEARN_SECONDS", "20")))
MAX_ROWS, MIN_ROWS, WINDOW = 8000, 100, 10
MIN_VALIDATION, MIN_HOLDOUT, MIN_TIME = 30, 30, 15
# Candidate cap prevents unbounded growth from repeated evolution.
MAX_SYSTEMS = max(300, int(os.getenv("XHAWON_MAX_SYSTEMS", "450")))
MAX_MINED_RULES = 75
MIN_CONTEXT_SUPPORT = 5
MIN_CONTEXT_EDGE = 0.60
QUALIFY = 60.0
TZ = timezone(timedelta(hours=6))
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("xhawon")
TASKS = []
last_analyzed_issue = None

def now(): return datetime.now(TZ).isoformat(timespec="seconds")
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=10000")
    return c

def init_db():
    with db() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS results(issue TEXT PRIMARY KEY, number INTEGER NOT NULL, kind TEXT NOT NULL, ts TEXT NOT NULL, collected_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS systems(system_id TEXT PRIMARY KEY,name TEXT,family TEXT,definition TEXT,params TEXT,status TEXT,version INTEGER,parent_id TEXT,created_at TEXT,updated_at TEXT);
        CREATE TABLE IF NOT EXISTS evaluations(system_id TEXT,dataset TEXT,samples INTEGER,wins INTEGER,losses INTEGER,accuracy REAL,updated_at TEXT,PRIMARY KEY(system_id,dataset));
        CREATE TABLE IF NOT EXISTS time_stats(system_id TEXT,hour INTEGER,samples INTEGER,wins INTEGER,accuracy REAL,updated_at TEXT,PRIMARY KEY(system_id,hour));
        CREATE TABLE IF NOT EXISTS failures(system_id TEXT PRIMARY KEY,failure_count INTEGER,reasons TEXT,updated_at TEXT);
        CREATE TABLE IF NOT EXISTS evolution(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id TEXT,child_id TEXT UNIQUE,reason TEXT,change_text TEXT,created_at TEXT);
        CREATE TABLE IF NOT EXISTS research_events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT,message TEXT,created_at TEXT);
        CREATE TABLE IF NOT EXISTS drift_events(id INTEGER PRIMARY KEY AUTOINCREMENT,system_id TEXT,old_accuracy REAL,new_accuracy REAL,old_n INTEGER,new_n INTEGER,created_at TEXT);
        CREATE INDEX IF NOT EXISTS idx_results_issue ON results(issue);
        """)

def event(kind, message):
    try:
        with db() as c:
            c.execute("INSERT INTO research_events(event_type,message,created_at) VALUES(?,?,?)",(kind,str(message)[:800],now()))
            c.execute("DELETE FROM research_events WHERE id NOT IN (SELECT id FROM research_events ORDER BY id DESC LIMIT 1000)")
    except Exception: log.exception("event save failed")

def pick(d, keys):
    for k in keys:
        if isinstance(d,dict) and d.get(k) not in (None,""): return d[k]
    return None

def parse_time(v):
    try:
        # Do not use collection time as a substitute for draw time: it would
        # make the "best hour" report misleading when the API omits timestamps.
        if v is None or str(v).strip() == "": return ""
        if isinstance(v,(int,float)):
            v=float(v); v=v/1000 if v>10_000_000_000 else v
            return datetime.fromtimestamp(v,TZ).isoformat(timespec="seconds")
        s=str(v).strip()
        if s.isdigit():
            numeric=float(s)
            numeric=numeric/1000 if numeric>10_000_000_000 else numeric
            return datetime.fromtimestamp(numeric,TZ).isoformat(timespec="seconds")
        s=s.replace("Z","+00:00")
        d=datetime.fromisoformat(s)
        if d.tzinfo is None: d=d.replace(tzinfo=TZ)
        return d.astimezone(TZ).isoformat(timespec="seconds")
    except Exception: return ""

def normalize(payload):
    data=payload.get("data",payload) if isinstance(payload,dict) else payload
    src=(data.get("list") or data.get("data") or data.get("records") or []) if isinstance(data,dict) else data if isinstance(data,list) else []
    out=[]
    for r in src:
        if not isinstance(r,dict): continue
        issue=pick(r,["issueNumber","issue","period","periodNumber","issueNo"])
        value=pick(r,["number","result","openNumber","num"])
        try: n=int(str(value).strip())
        except Exception: continue
        if issue is not None and 0<=n<=9:
            out.append((str(issue),n,"BIG" if n>=5 else "SMALL",parse_time(pick(r,["time","timestamp","drawTime","createdAt","date"]))))
    if src and not out: log.warning("API rows found but schema was not recognized")
    return out

async def fetch(session):
    async with session.get(API_URL,params={"pageNo":1,"pageSize":50,"t":int(time.time()*1000)},timeout=aiohttp.ClientTimeout(total=15)) as r:
        r.raise_for_status()
        return await r.json(content_type=None)

def save(rows):
    added=0
    with db() as c:
        for issue,n,k,ts in rows:
            cur=c.execute("INSERT OR IGNORE INTO results VALUES(?,?,?,?,?)",(issue,n,k,ts,now()))
            added+=max(cur.rowcount,0)
        c.execute("DELETE FROM results WHERE issue NOT IN (SELECT issue FROM results ORDER BY CAST(issue AS INTEGER) DESC LIMIT ?)",(MAX_ROWS,))
    return added

def history():
    with db() as c: rows=c.execute("SELECT issue,number,kind,ts FROM results ORDER BY CAST(issue AS INTEGER)").fetchall()
    return [dict(r) for r in rows]

def big(n): return int(n>=5)

# Seed rules from the user's supplied specification
def seed_number(nums):
    if len(nums)<10:return None
    x=nums[-10]; mapping={1:1,2:0,3:1,4:1,5:0,6:1,7:0,8:0,9:0}
    if x==0:return int(nums[-1] in (1,3,6,8))
    base=mapping.get(x)
    return None if base is None else (1-base if nums[-10:].count(x)>1 else base)

def seed_plus(nums):
    if len(nums)<10:return None
    a,b=nums[-10],nums[-1]
    if a==0 or b==0:return None
    s=a+b
    return 0 if 1<=s<=4 or s==10 else 1 if 5<=s<=9 else None

def formula(nums,p):
    if len(nums)<10:return None
    a,b,x=nums[-10],nums[-9],nums[-1]
    if p.get("condition")=="nonzero" and 0 in (a,b,x):return None
    op=p["operation"]
    val={"sum":a+b,"difference":abs(a-b),"sum_last":a+x,"difference_last":abs(a-x),"weighted":2*a+b,"product_digit_sum":sum(map(int,str(a*b)))}[op]
    mode=p["mapping"]
    if mode=="parity":return val%2
    if mode=="big_small":return int(val%10>=5)
    return None

def predict(nums,p):
    f=p["family"]; w=int(p.get("window",3))
    if f=="seed_number":return seed_number(nums)
    if f=="seed_plus":return seed_plus(nums)
    if f=="formula":return formula(nums,p)
    if f=="discovered_formula":
        la,lb=int(p["lag_a"]),int(p["lag_b"])
        if len(nums)<max(la,lb):return None
        a,b=nums[-la],nums[-lb]; op=p["operation"]
        val=(a+b if op=="sum" else abs(a-b) if op=="difference" else sum(map(int,str(a*b))))
        mapping=p["mapping"]
        return val%2 if mapping=="parity" else int(val%10>=5) if mapping=="big_small" else int(val%3==0)
    if f=="mined_context":
        ctx=p["context"]
        if len(nums)<len(ctx):return None
        actual="".join(str(big(x)) for x in nums[-len(ctx):])
        return int(p["outcome"]) if actual==ctx else None
    if len(nums)<max(3,w):return None
    nums=nums[-w:]; bits=[big(x) for x in nums]
    if f in ("majority","minority"):
        s=sum(bits)
        if s*2==w:return None
        v=int(s*2>w)
        return v if f=="majority" else 1-v
    if f in ("streak_reverse","streak_follow"):
        if len(set(bits))!=1:return None
        return 1-bits[-1] if f=="streak_reverse" else bits[-1]
    if f in ("alternation","alternation_follow"):
        if not all(bits[i]!=bits[i-1] for i in range(1,len(bits))):return None
        return 1-bits[-1] if f=="alternation" else bits[-1]
    if f=="recent_pressure":
        s=sum(bits)
        return 1 if s>=p["high"] else 0 if s<=p["low"] else None
    if f=="digit_pressure":return p["outcome"] if nums.count(p["digit"])>=p["count"] else None
    if f=="position_relation":
        ok=(nums[0]==nums[-1]) if p["relation"]=="same" else (nums[0]!=nums[-1])
        return p["outcome"] if ok else None
    if f=="kind_run":
        run=1
        for z in reversed(bits[:-1]):
            if z!=bits[-1]:break
            run+=1
        return p["outcome"] if run==p["run"] else None
    return None

def predict_at(nums, end, p):
    """Predict the result at index `end` using only nums[:end], without copying
    the entire growing prefix on every backtest step."""
    f=p["family"]
    w=int(p.get("window",3))
    if end < WINDOW and f in ("seed_number","seed_plus","formula"):
        return None
    if f == "seed_number":
        a=end-WINDOW
        x=nums[a]
        if x==0:
            return int(nums[end-1] in (1,3,6,8))
        mapping={1:1,2:0,3:1,4:1,5:0,6:1,7:0,8:0,9:0}
        base=mapping.get(x)
        return None if base is None else (1-base if sum(1 for j in range(a,end) if nums[j]==x)>1 else base)
    if f == "seed_plus":
        a,b=nums[end-WINDOW],nums[end-1]
        if a==0 or b==0:return None
        s=a+b
        return 0 if 1<=s<=4 or s==10 else 1 if 5<=s<=9 else None
    if f == "formula":
        a,b,x=nums[end-WINDOW],nums[end-WINDOW+1],nums[end-1]
        if p.get("condition")=="nonzero" and 0 in (a,b,x):return None
        op=p["operation"]
        val={"sum":a+b,"difference":abs(a-b),"sum_last":a+x,
             "difference_last":abs(a-x),"weighted":2*a+b,
             "product_digit_sum":sum(map(int,str(a*b)))}[op]
        return val%2 if p["mapping"]=="parity" else int(val%10>=5) if p["mapping"]=="big_small" else None
    if f == "discovered_formula":
        la,lb=int(p["lag_a"]),int(p["lag_b"])
        if end<max(la,lb):return None
        a,b=nums[end-la],nums[end-lb]; op=p["operation"]
        val=(a+b if op=="sum" else abs(a-b) if op=="difference" else sum(map(int,str(a*b))))
        mapping=p["mapping"]
        return val%2 if mapping=="parity" else int(val%10>=5) if mapping=="big_small" else int(val%3==0)
    if f == "mined_context":
        ctx=p["context"]
        if end<len(ctx):return None
        actual="".join(str(big(x)) for x in nums[end-len(ctx):end])
        return int(p["outcome"]) if actual==ctx else None
    if end < max(3,w):return None
    start=end-w
    tail=nums[start:end]
    bits_=[big(x) for x in tail]
    if f in ("majority","minority"):
        total=sum(bits_)
        if total*2==w:return None
        v=int(total*2>w)
        return v if f=="majority" else 1-v
    if f in ("streak_reverse","streak_follow"):
        if len(set(bits_))!=1:return None
        return 1-bits_[-1] if f=="streak_reverse" else bits_[-1]
    if f in ("alternation","alternation_follow"):
        if not all(bits_[i]!=bits_[i-1] for i in range(1,len(bits_))):return None
        return 1-bits_[-1] if f=="alternation" else bits_[-1]
    if f=="recent_pressure":
        total=sum(bits_)
        return 1 if total>=p["high"] else 0 if total<=p["low"] else None
    if f=="digit_pressure":
        return p["outcome"] if tail.count(p["digit"])>=p["count"] else None
    if f=="position_relation":
        ok=(tail[0]==tail[-1]) if p["relation"]=="same" else (tail[0]!=tail[-1])
        return p["outcome"] if ok else None
    if f=="kind_run":
        run=1
        for z in reversed(bits_[:-1]):
            if z!=bits_[-1]:break
            run+=1
        return p["outcome"] if run==p["run"] else None
    return None

def desc(p):
    f=p["family"]
    if f=="seed_number":return "First position digit mapping; repeated first digit reverses signal; zero has a separate rule."
    if f=="seed_plus":return "First + latest digit; zero means no trade; sum maps to BIG/SMALL."
    if f=="formula":return f"Arithmetic formula: {p['operation']} → {p['mapping']}; condition={p['condition']}."
    if f=="discovered_formula":return f"Auto-generated arithmetic rule: lag {p['lag_a']} and lag {p['lag_b']}, {p['operation']} → {p['mapping']}."
    if f=="mined_context":return f"Data-mined BIG/SMALL context rule: previous context {p['context']} → {'BIG' if int(p['outcome']) else 'SMALL'}; discovered from training split, then tested on later data."
    return json.dumps(p,ensure_ascii=False,sort_keys=True)

def pool():
    out=[("SEED_NUMBER","Seed Number","seed_number",{"family":"seed_number"}),("SEED_PLUS","Seed Plus","seed_plus",{"family":"seed_plus"})]
    for w in range(3,11):
        for code,fam,label in [("MAJ","majority","Majority"),("MIN","minority","Minority"),("SR","streak_reverse","Streak reverse"),("SF","streak_follow","Streak follow"),("ALT","alternation","Alternation reverse"),("ALTF","alternation_follow","Alternation follow")]:
            out.append((f"{code}{w}",f"{label} w{w}","sequence",{"family":fam,"window":w}))
    for w in range(4,11):
        out.append((f"PRESS{w}",f"Recent pressure w{w}","pressure",{"family":"recent_pressure","window":w,"high":w-1,"low":1}))
    for d in range(10):
        for count in (2,3,4):
            for o in (0,1):out.append((f"D{d}_{count}_{o}",f"Digit {d} x{count} -> {o}","digit",{"family":"digit_pressure","window":10,"digit":d,"count":count,"outcome":o}))
    for rel in ("same","different"):
        for w in (5,10):
            for o in (0,1):out.append((f"POS{rel}{w}{o}",f"Position {rel} w{w}","position",{"family":"position_relation","window":w,"relation":rel,"outcome":o}))
    for run in (2,3,4,5):
        for o in (0,1):out.append((f"RUN{run}{o}",f"Kind run {run}","run",{"family":"kind_run","window":10,"run":run,"outcome":o}))
    for op in ("sum","difference","sum_last","difference_last","weighted","product_digit_sum"):
        for mapping in ("parity","big_small"):
            for cond in ("always","nonzero"):
                p={"family":"formula","operation":op,"mapping":mapping,"condition":cond}
                out.append((f"FORM{op}{mapping}{cond}",f"Formula {op}/{mapping}/{cond}","formula",p))
    # A generated grammar searches combinations of historical digit lags,
    # arithmetic operations and binary mappings instead of only mutating seeds.
    lags=(1,2,3,5,10)
    for ai,la in enumerate(lags):
        for lb in lags[ai+1:]:
            for op in ("sum","difference","product_digit_sum"):
                for mapping in ("parity","big_small","mod3_zero"):
                    p={"family":"discovered_formula","lag_a":la,"lag_b":lb,"operation":op,"mapping":mapping}
                    out.append((f"AUTO_{la}_{lb}_{op}_{mapping}",f"Auto formula lag {la}/{lb} {op}/{mapping}","discovered_formula",p))
    return out

def sid(cid,p):return "SYS_"+hashlib.sha256((cid+json.dumps(p,sort_keys=True)).encode()).hexdigest()[:14]

def register():
    t=now()
    with db() as c:
        for cid,name,fam,p in pool():
            c.execute("INSERT OR IGNORE INTO systems VALUES(?,?,?,?,?,'TESTING',1,NULL,?,?)",(sid(cid,p),name,fam,desc(p),json.dumps(p),t,t))
    event("DISCOVERY",f"Registered {len(pool())} seed and generated-grammar candidates.")

def discover_from_training(rows):
    """Mine conditional BIG/SMALL context rules only from the first 60% of
    the timeline. Validation and holdout remain later, unseen periods."""
    n=len(rows)
    if n<MIN_ROWS:return 0
    train_end=max(WINDOW+1,int(n*.60))
    bits=[big(r["number"]) for r in rows[:train_end]]
    counts={}
    for k in range(2,6):
        for i in range(k, len(bits)):
            ctx="".join(str(x) for x in bits[i-k:i])
            slot=counts.setdefault((ctx,bits[i]),[0,0])
            slot[0]+=1
        # outcome counts stored as (support, wins for that outcome)
    candidates=[]
    contexts={ctx for ctx,_ in counts}
    for ctx in contexts:
        c0=counts.get((ctx,0),[0,0])[0]
        c1=counts.get((ctx,1),[0,0])[0]
        total=c0+c1
        if total<MIN_CONTEXT_SUPPORT:continue
        outcome=1 if c1>c0 else 0
        edge=max(c0,c1)/total
        if edge<MIN_CONTEXT_EDGE:continue
        candidates.append((edge,total,ctx,outcome))
    candidates.sort(reverse=True)
    with db() as c:
        existing=c.execute("SELECT COUNT(*) FROM systems WHERE family='mined_context'").fetchone()[0]
        room=max(0,MAX_MINED_RULES-existing)
        overall=max(0,MAX_SYSTEMS-c.execute("SELECT COUNT(*) FROM systems").fetchone()[0])
        room=min(room,overall)
        created=0
        for edge,support,ctx,outcome in candidates:
            if created>=room:break
            p={"family":"mined_context","context":ctx,"outcome":outcome}
            cid=f"MINE_{ctx}_{outcome}"
            system_id=sid(cid,p)
            cur=c.execute("INSERT OR IGNORE INTO systems VALUES(?,?,?,?,?,'TESTING',1,NULL,?,?)",
                (system_id,f"Mined context {ctx} -> {outcome}","mined_context",desc(p),json.dumps(p),now(),now()))
            if cur.rowcount:
                created+=1
                c.execute("INSERT INTO research_events(event_type,message,created_at) VALUES(?,?,?)",
                    ("PATTERN_DISCOVERED",f"Created context rule {ctx} -> {'BIG' if outcome else 'SMALL'}; train support={support}, in-sample share={edge*100:.1f}%. Must pass validation and holdout.",now()))
    return created

def eval_range(nums,rows,p,start,end):
    wins=losses=0; hours={h:[0,0] for h in range(24)}
    for i in range(max(start,WINDOW),end):
        pred=predict_at(nums,i,p)
        if pred is None:continue
        ok=pred==big(nums[i]); wins+=int(ok);losses+=int(not ok)
        try:
            h=datetime.fromisoformat(rows[i]["ts"]).hour;hours[h][0]+=1;hours[h][1]+=int(ok)
        except Exception:pass
    n=wins+losses
    return wins,losses,(100*wins/n if n else 0.0),hours

def put_eval(sid_,dataset,w,l,a):
    with db() as c:
        c.execute("""INSERT INTO evaluations VALUES(?,?,?,?,?,?,?) ON CONFLICT(system_id,dataset) DO UPDATE SET
        samples=excluded.samples,wins=excluded.wins,losses=excluded.losses,accuracy=excluded.accuracy,updated_at=excluded.updated_at""",
        (sid_,dataset,w+l,w,l,a,now()))

def analyze():
    rows=history(); n=len(rows)
    if n<MIN_ROWS:return
    nums=[r["number"] for r in rows]; tr=int(n*.60); va=int(n*.80)
    with db() as c: systems=c.execute("SELECT * FROM systems").fetchall()
    for s in systems:
        p=json.loads(s["params"])
        tw,tl,ta,_=eval_range(nums,rows,p,WINDOW,tr)
        vw,vl,vacc,_=eval_range(nums,rows,p,tr,va)
        hw,hl,hacc,hours=eval_range(nums,rows,p,va,n)
        put_eval(s["system_id"],"train",tw,tl,ta);put_eval(s["system_id"],"validation",vw,vl,vacc);put_eval(s["system_id"],"holdout",hw,hl,hacc)
        with db() as c:
            c.execute("DELETE FROM time_stats WHERE system_id=?",(s["system_id"],))
            for h,(samples,wins) in hours.items():
                if samples:c.execute("INSERT INTO time_stats VALUES(?,?,?,?,?,?)",(s["system_id"],h,samples,wins,100*wins/samples,now()))
        if vw+vl>=MIN_VALIDATION and hw+hl>=MIN_HOLDOUT and vacc>=QUALIFY and hacc>=QUALIFY:status="QUALIFIED"
        elif vw+vl>=MIN_VALIDATION and hw+hl>=MIN_HOLDOUT and (vacc<50 or hacc<50):status="EVOLVING"
        else:status="TESTING"
        with db() as c:c.execute("UPDATE systems SET status=?,updated_at=? WHERE system_id=?",(status,now(),s["system_id"]))
        # Failure notes are context hints, not proof of causation.
        reasons=Counter()
        for i in range(max(va,WINDOW),n):
            pred=predict_at(nums,i,p)
            if pred is not None and pred!=big(nums[i]):
                recent=[big(x) for x in nums[max(0,i-3):i]]
                if len(recent)==3 and len(set(recent))==1:reasons["three same BIG/SMALL results"]+=1
                if i and nums[i-1]==nums[i]:reasons["same digit repeated"]+=1
        if reasons:
            with db() as c:c.execute("""INSERT INTO failures VALUES(?,?,?,?) ON CONFLICT(system_id) DO UPDATE SET
            failure_count=excluded.failure_count,reasons=excluded.reasons,updated_at=excluded.updated_at""",
            (s["system_id"],sum(reasons.values()),json.dumps(dict(reasons),ensure_ascii=False),now()))

def mutate(p):
    variants=[]
    if "window" in p:
        for d in (-1,1):
            q=dict(p);q["window"]=max(3,min(10,int(p["window"])+d));variants.append((q,f"window {p['window']} -> {q['window']}"))
    if p["family"] in ("streak_reverse","streak_follow"):
        q=dict(p);q["family"]="streak_follow" if p["family"]=="streak_reverse" else "streak_reverse";variants.append((q,"streak response flipped"))
    if p["family"]=="formula":
        q=dict(p);q["mapping"]="big_small" if p["mapping"]=="parity" else "parity";variants.append((q,"formula output mapping changed"))
    if p["family"]=="discovered_formula":
        for key,vals in (("lag_a",(1,2,3,5,10)),("lag_b",(1,2,3,5,10))):
            q=dict(p); choices=[v for v in vals if v!=p[key]]; q[key]=choices[(vals.index(p[key])+1)%len(choices)];
            if q["lag_a"]!=q["lag_b"]:
                if q["lag_a"]>q["lag_b"]:q["lag_a"],q["lag_b"]=q["lag_b"],q["lag_a"]
                variants.append((q,f"{key} mutated"))
        q=dict(p);q["mapping"]={"parity":"big_small","big_small":"mod3_zero","mod3_zero":"parity"}[p["mapping"]];variants.append((q,"arithmetic output mapping changed"))
    if p["family"]=="mined_context":
        q=dict(p);q["outcome"]=1-int(p["outcome"]);variants.append((q,"context outcome flipped for independent testing"))
        ctx=p["context"]
        if ctx:
            q=dict(p);q["context"]=ctx[:-1]+("1" if ctx[-1]=="0" else "0");variants.append((q,"last context bit mutated"))
    unique=[];seen=set()
    for q,txt in variants:
        key=json.dumps(q,sort_keys=True)
        if key not in seen and q!=p:seen.add(key);unique.append((q,txt))
    return unique[:2]

def evolve():
    with db() as c:
        current=c.execute("SELECT COUNT(*) FROM systems").fetchone()[0]
        # Keep capacity available for future data-mined rules; evolution must
        # not permanently crowd out autonomous discovery.
        available=MAX_SYSTEMS-MAX_MINED_RULES-current
        if available<=0:
            return 0
        parents=c.execute("""SELECT s.* FROM systems s JOIN evaluations v ON v.system_id=s.system_id AND v.dataset='validation'
        JOIN evaluations h ON h.system_id=s.system_id AND h.dataset='holdout'
        WHERE s.status IN ('EVOLVING','TESTING') AND v.samples>=? AND h.samples>=?
        ORDER BY COALESCE(v.accuracy,0) ASC LIMIT 12""",(MIN_VALIDATION,MIN_HOLDOUT)).fetchall()
    created=0
    for parent in parents:
        if created>=available: break
        for p,change in mutate(json.loads(parent["params"])):
            if created>=available: break
            child=parent["system_id"]+"_V"+str(parent["version"]+1)+"_"+hashlib.sha1(json.dumps(p,sort_keys=True).encode()).hexdigest()[:5]
            with db() as c:
                cur=c.execute("""INSERT OR IGNORE INTO systems VALUES(?,?,?,?,?,'TESTING',?,?,?,?)""",
                    (child,parent["name"]+" evolved",p["family"],desc(p),json.dumps(p),parent["version"]+1,parent["system_id"],now(),now()))
                if cur.rowcount:
                    c.execute("INSERT OR IGNORE INTO evolution(parent_id,child_id,reason,change_text,created_at) VALUES(?,?,?,?,?)",
                              (parent["system_id"],child,"Candidate mutation; improvement not assumed",change,now()))
                    created+=1
    return created

def qualified():
    with db() as c:return c.execute("""SELECT s.*,v.accuracy va,v.samples vn,h.accuracy ha,h.samples hn,h.wins hw,h.losses hl
        FROM systems s JOIN evaluations v ON v.system_id=s.system_id AND v.dataset='validation'
        JOIN evaluations h ON h.system_id=s.system_id AND h.dataset='holdout'
        WHERE s.status='QUALIFIED' ORDER BY h.accuracy DESC,h.samples DESC LIMIT 10""").fetchall()

def detail(sid_):
    with db() as c:
        s=c.execute("SELECT * FROM systems WHERE system_id=?",(sid_,)).fetchone()
        if not s:return "Logic not found."
        ev=c.execute("SELECT * FROM evaluations WHERE system_id=?",(sid_,)).fetchall()
        tm=c.execute("SELECT * FROM time_stats WHERE system_id=? AND samples>=? ORDER BY accuracy DESC LIMIT 3",(sid_,MIN_TIME)).fetchall()
        f=c.execute("SELECT reasons FROM failures WHERE system_id=?",(sid_,)).fetchone()
        e=c.execute("SELECT change_text,created_at FROM evolution WHERE child_id=? ORDER BY id",(sid_,)).fetchall()
    lines=[f"🧠 {s['name']}",f"ID: {sid_}",f"Family: {s['family']} | v{s['version']} | {s['status']}","", "📌 LOGIC",desc(json.loads(s["params"])),""]
    lines += [f"{x['dataset'].upper()}: {x['accuracy']:.1f}% | n={x['samples']} | W={x['wins']} L={x['losses']}" for x in ev]
    if tm:lines += ["","⏰ Best observed Bangladesh-time hours:"]+[f"{x['hour']:02d}:00 — {x['accuracy']:.1f}% (n={x['samples']})" for x in tm]
    if f:
        try:lines += ["","🔎 Failure context (not proven causes):"]+[f"{k}: {v}" for k,v in json.loads(f["reasons"]).items()]
        except Exception:pass
    if e:lines += ["","🧬 Evolution history"]+[f"{x['created_at']}: {x['change_text']}" for x in e]
    lines += ["","⚠️ Historical/paper testing only; no future outcome is guaranteed."]
    return "\n".join(lines)

def status_text():
    rows=history()
    with db() as c:
        counts={x["status"]:x["n"] for x in c.execute("SELECT status,COUNT(*) n FROM systems GROUP BY status").fetchall()}
        total=c.execute("SELECT COUNT(*) FROM systems").fetchone()[0]
        mined=c.execute("SELECT COUNT(*) FROM systems WHERE family='mined_context'").fetchone()[0]
        generated=c.execute("SELECT COUNT(*) FROM systems WHERE family='discovered_formula'").fetchone()[0]
    last=rows[-1] if rows else None
    return (f"🧠 XHAWON AI v8.0\n📡 Periods stored: {len(rows)}\n🧩 Registered candidates: {total}\n"
            f"🧮 Generated formula rules: {generated}\n🔎 Data-mined context rules: {mined}\n"
            f"✅ Qualified: {counts.get('QUALIFIED',0)}\n🧬 Evolving: {counts.get('EVOLVING',0)}\n"
            f"🧪 Testing: {counts.get('TESTING',0)}\n📌 Last: {last['number']} / {last['kind']} (issue {last['issue']})" if last else
            f"🧠 XHAWON AI v8.0\n📡 Periods stored: 0\n🧩 Registered candidates: {total}\nWaiting for market data")

def report_text():
    q=qualified()
    if q:return "📚 LEARNING REPORT\n\n"+"\n\n────────────\n\n".join(detail(x["system_id"]) for x in q[:5])
    with db() as c:
        fail=c.execute("SELECT COUNT(*) FROM failures").fetchone()[0];evo=c.execute("SELECT COUNT(*) FROM evolution").fetchone()[0]
        rows=c.execute("""SELECT s.name,s.status,v.samples vn,v.accuracy va,h.samples hn,h.accuracy ha FROM systems s
        LEFT JOIN evaluations v ON v.system_id=s.system_id AND v.dataset='validation'
        LEFT JOIN evaluations h ON h.system_id=s.system_id AND h.dataset='holdout'
        ORDER BY COALESCE(v.accuracy,0) LIMIT 8""").fetchall()
    out=["📚 LEARNING REPORT","এখনো কোনো candidate validation ও holdout—দুইটিতেই শর্ত পূরণ করেনি.",f"Failure records: {fail}",f"Evolution candidates created: {evo}","","Research candidates:"]
    for r in rows:out.append(f"• {r['name']} [{r['status']}] val={r['va'] if r['va'] is not None else 'N/A'}% n={r['vn'] or 0}; hold={r['ha'] if r['ha'] is not None else 'N/A'}% n={r['hn'] or 0}")
    out += ["","Note: registered candidates are not the same as completed tests."]
    return "\n".join(out)

def time_text():
    with db() as c:r=c.execute("""SELECT s.name,t.hour,t.accuracy,t.samples FROM time_stats t JOIN systems s ON s.system_id=t.system_id
        WHERE s.status='QUALIFIED' AND t.samples>=? ORDER BY t.accuracy DESC LIMIT 12""",(MIN_TIME,)).fetchall()
    return "⏰ BEST OBSERVED HOURS\n\n"+"\n".join(f"{x['name']} | {x['hour']:02d}:00 | {x['accuracy']:.1f}% | n={x['samples']}" for x in r) if r else "⏰ Qualified logic-এর পর্যাপ্ত hour-wise sample নেই."

def discoveries_text():
    with db() as c:
        rules=c.execute("SELECT * FROM systems WHERE family='mined_context' ORDER BY created_at DESC LIMIT 20").fetchall()
        formulas=c.execute("SELECT COUNT(*) FROM systems WHERE family='discovered_formula'").fetchone()[0]
        total=c.execute("SELECT COUNT(*) FROM systems WHERE family='mined_context'").fetchone()[0]
    lines=["🧬 AUTONOMOUSLY DISCOVERED PATTERNS",f"Data-mined context rules: {total}/{MAX_MINED_RULES}",f"Generated arithmetic-rule templates: {formulas}","","Latest data-mined rules:"]
    if rules:
        for r in rules:
            p=json.loads(r["params"])
            lines.append(f"• {r['name']} | {r['status']} | {desc(p)}")
    else:lines.append("এখনো পর্যাপ্ত training data থেকে নতুন context rule তৈরি হয়নি.")
    lines += ["","প্রতিটি rule-এর validation ও holdout ফল না দেখে এটিকে নির্ভরযোগ্য বলা যাবে না."]
    return "\n".join(lines)

def activity_text():
    with db() as c:r=c.execute("SELECT * FROM research_events ORDER BY id DESC LIMIT 12").fetchall()
    return "🔬 RESEARCH\n\n"+"\n\n".join(f"{x['created_at']} | {x['event_type']}\n{x['message']}" for x in r) if r else "Research activity এখনো নেই."

def health_text():
    rows=history()
    with db() as c:
        systems=c.execute("SELECT COUNT(*) FROM systems").fetchone()[0]
        evals=c.execute("SELECT COUNT(*) FROM evaluations").fetchone()[0]
        events=c.execute("SELECT COUNT(*) FROM research_events").fetchone()[0]
        db_bytes=os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        latest=c.execute("SELECT created_at FROM research_events ORDER BY id DESC LIMIT 1").fetchone()
    try:
        with db() as c: check=c.execute("PRAGMA quick_check").fetchone()[0]
    except Exception as e: check=f"ERROR: {e}"
    last=rows[-1] if rows else None
    return ("🩺 XHAWON HEALTH CHECK\n"
        f"Python: {platform.python_version()}\nDatabase: {check}\nDB size: {db_bytes/1024:.1f} KB\n"
        f"Stored results: {len(rows)}\nSystems: {systems} | evaluation rows: {evals}\nResearch events: {events}\n"
        f"Last result: {last['issue'] if last else 'none'}\nLast event: {latest['created_at'] if latest else 'none'}\n"
        f"API endpoint configured: {'yes' if API_URL.startswith('https://') else 'check URL'}\n"
        "Note: this checks local state; it does not prove the upstream API or Telegram is reachable.")

def wilson_interval(wins, samples, z=1.96):
    if samples <= 0:return (0.0, 0.0)
    p=wins/samples; den=1+z*z/samples
    center=(p+z*z/(2*samples))/den
    margin=z*math.sqrt((p*(1-p)+z*z/(4*samples))/samples)/den
    return (max(0.0,center-margin)*100,min(1.0,center+margin)*100)

def leaderboard_text():
    with db() as c:
        rows=c.execute("""SELECT s.name,s.family,s.status,v.samples vn,v.accuracy va,h.samples hn,h.accuracy ha,h.wins hw
        FROM systems s LEFT JOIN evaluations v ON v.system_id=s.system_id AND v.dataset='validation'
        LEFT JOIN evaluations h ON h.system_id=s.system_id AND h.dataset='holdout'
        WHERE COALESCE(v.samples,0)>0 AND COALESCE(h.samples,0)>0
        ORDER BY CASE WHEN s.status='QUALIFIED' THEN 0 ELSE 1 END, COALESCE(h.accuracy,0) DESC, COALESCE(h.samples,0) DESC LIMIT 15""").fetchall()
    if not rows:return "🏆 LEADERBOARD\nএখনো পর্যাপ্ত ডাটা দিয়ে backtest সম্পন্ন হয়নি।"
    out=["🏆 MODEL LEADERBOARD (validation + holdout)","Rank is historical performance, not a future guarantee.",""]
    for i,r in enumerate(rows,1):
        lo,hi=wilson_interval(r['hw'] or 0,r['hn'] or 0)
        out.append(f"{i}. {r['name']} [{r['status']}]\n   validation {r['va']:.1f}% (n={r['vn']}); holdout {r['ha']:.1f}% (n={r['hn']}, approximate 95% interval {lo:.1f}–{hi:.1f}%)")
    return "\n".join(out)

def backtest_text():
    rows=history(); n=len(rows)
    if n<MIN_ROWS:return f"🔬 WALK-FORWARD BACKTEST\nNeed at least {MIN_ROWS} stored results; currently {n}."
    nums=[r['number'] for r in rows]; tr=int(n*.60); va=int(n*.80)
    sections=[("TRAIN",WINDOW,tr),("VALIDATION",tr,va),("HOLDOUT",va,n)]
    out=["🔬 CHRONOLOGICAL BACKTEST SUMMARY",f"Total rows: {n}; split: 60% / 20% / 20%","Predictions use only earlier history at each test step.",""]
    for label,a,b in sections:
        vals=[big(nums[i]) for i in range(max(a,WINDOW),b)]
        if vals:
            rate=sum(vals)/len(vals)*100
            out.append(f"{label}: evaluated {len(vals)} outcomes; majority-class baseline={max(rate,100-rate):.1f}% (BIG share={rate:.1f}%)")
        else:out.append(f"{label}: not enough outcomes")
    out += ["", "This baseline is not a strategy score. Compare each model to a proper baseline on the same exact timestamps and sample; no guarantee of predictive edge."]
    return "\n".join(out)

def drift_text():
    rows=history(); n=len(rows)
    if n<MIN_ROWS:return f"📉 DRIFT MONITOR\nNeed {MIN_ROWS} rows; currently {n}."
    nums=[r['number'] for r in rows]; mid=max(WINDOW,n-200); start=max(WINDOW,n-100)
    with db() as c:
        systems=c.execute("SELECT * FROM systems WHERE status='QUALIFIED' ORDER BY system_id LIMIT 30").fetchall()
    lines=["📉 RECENT PERFORMANCE / DRIFT CHECK",f"Recent window: last {n-start} outcomes; prior comparison window ends before that.",""]
    detected=0
    for s in systems:
        p=json.loads(s['params'])
        ow,ol,oa,_=eval_range(nums,rows,p,max(WINDOW,n-200),start)
        nw,nl,na,_=eval_range(nums,rows,p,start,n)
        if ow+ol<20 or nw+nl<20:continue
        delta=na-oa
        label="POSSIBLE DECLINE" if delta<=-10 else "possible improvement" if delta>=10 else "no large change"
        if delta<=-10:
            detected+=1
            with db() as c:c.execute("INSERT INTO drift_events(system_id,old_accuracy,new_accuracy,old_n,new_n,created_at) VALUES(?,?,?,?,?,?)",(s['system_id'],oa,na,ow+ol,nw+nl,now()))
        lines.append(f"{s['name']}: prior {oa:.1f}% (n={ow+ol}) → recent {na:.1f}% (n={nw+nl}); {delta:+.1f} pp — {label}")
    if len(lines)==3:lines.append("No qualified system has enough samples for this comparison.")
    lines += ["",f"Decline flags this run: {detected}. These are monitoring alerts, not proof of a real predictive regime change."]
    return "\n".join(lines)

def registry_text():
    with db() as c:
        rows=c.execute("SELECT status,family,COUNT(*) n FROM systems GROUP BY status,family ORDER BY status,family").fetchall()
        ev=c.execute("SELECT COUNT(*) FROM evolution").fetchone()[0]
        mined=c.execute("SELECT COUNT(*) FROM systems WHERE family='mined_context'").fetchone()[0]
    out=["🗂 MODEL REGISTRY",f"Total evolution lineage entries: {ev}",f"Autonomous mined-context rules: {mined}",""]
    out += [f"{r['status']} / {r['family']}: {r['n']}" for r in rows]
    out += ["", "Candidate version and parent links are stored in SQLite systems/evolution tables."]
    return "\n".join(out)

async def cmd_health(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(health_text))
async def cmd_time(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(time_text))
async def cmd_leaderboard(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(leaderboard_text))
async def cmd_backtest(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(backtest_text))
async def cmd_drift(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(drift_text))
async def cmd_registry(update,context):
    if allowed(update):await send_chunks(update.effective_message,await asyncio.to_thread(registry_text))
async def cmd_help(update,context):
    if not allowed(update):return
    await update.effective_message.reply_text(
        "🧠 XHAWON AI v8.0 — COMMANDS\n"
        "/status — collection/model status\n/health — local database health\n"
        "/discover — autonomously mined rules\n/logic — qualified candidates\n"
        "/learning — detailed learning report\n/leaderboard — validation/holdout ranking\n"
        "/backtest — chronological split summary\n/drift — recent-window degradation check\n"
        "/registry — candidate families and evolution count\n/time — observed hour-wise results\n"
        "/export — download result CSV\n/backup — download SQLite research database\n"
        "/help — this menu\n\nHistorical research only; no guaranteed predictions or profits.")

async def cmd_backup(update,context):
    if not allowed(update):return
    try:
        if not os.path.exists(DB_PATH):
            await update.effective_message.reply_text("Database তৈরি হয়নি; bot initialization সম্পন্ন হয়েছে কি না দেখুন.");return
        fd,path=tempfile.mkstemp(prefix="xhawon_backup_",suffix=".sqlite3");os.close(fd)
        try:
            src=sqlite3.connect(DB_PATH,timeout=30);dst=sqlite3.connect(path)
            try:src.backup(dst)
            finally:dst.close();src.close()
            with open(path,"rb") as f: data=f.read()
            bio=io.BytesIO(data);bio.name="xhawon_research_backup.sqlite3"
            await update.effective_message.reply_document(document=bio,caption="XHAWON SQLite backup. Keep it private; contains historical data and research logs.")
        finally:
            try:os.remove(path)
            except OSError:pass
    except Exception:
        log.exception("backup failed");await update.effective_message.reply_text("Backup ব্যর্থ হয়েছে; hosting log দেখুন।")

async def cmd_export(update,context):
    if not allowed(update):return
    try:
        rows=await asyncio.to_thread(history)
        if not rows:
            await update.effective_message.reply_text("Export করার মতো result নেই।");return
        buf=io.StringIO(newline="")
        writer=csv.DictWriter(buf,fieldnames=["issue","number","kind","ts","collected_at"])
        writer.writeheader()
        with db() as c: raw=c.execute("SELECT issue,number,kind,ts,collected_at FROM results ORDER BY CAST(issue AS INTEGER)").fetchall()
        writer.writerows([dict(x) for x in raw])
        bio=io.BytesIO(buf.getvalue().encode("utf-8-sig"));bio.name="xhawon_results_export.csv"
        await update.effective_message.reply_document(document=bio,caption=f"XHAWON results export — {len(raw)} rows")
    except Exception:
        log.exception("export failed");await update.effective_message.reply_text("Export ব্যর্থ হয়েছে; hosting log দেখুন।")

def allowed(update):return bool(ADMIN_CHAT_ID and update.effective_chat and str(update.effective_chat.id)==ADMIN_CHAT_ID)
def menu():return InlineKeyboardMarkup([[InlineKeyboardButton("📊 Status",callback_data="status"),InlineKeyboardButton("🧠 Logic",callback_data="logic")],[InlineKeyboardButton("📚 Learning Report",callback_data="report"),InlineKeyboardButton("🧬 Discovered Patterns",callback_data="discoveries")],[InlineKeyboardButton("🔬 Research",callback_data="activity"),InlineKeyboardButton("⏰ Best Time",callback_data="time")],[InlineKeyboardButton("🏆 Leaderboard",callback_data="leaderboard"),InlineKeyboardButton("🩺 Health",callback_data="health")],[InlineKeyboardButton("📉 Drift",callback_data="drift"),InlineKeyboardButton("🗂 Registry",callback_data="registry")]])

async def send_chunks(msg,text):
    while text:
        cut=3800
        if len(text)<=cut:await msg.reply_text(text);break
        pos=text.rfind("\n",0,cut)
        if pos<1:pos=cut
        await msg.reply_text(text[:pos]);text=text[pos:].lstrip()

async def start(update,context):
    if allowed(update) and update.message:
        await update.message.reply_text("🧠 XHAWON SELF-LEARNING PATTERN AI v8.0\nডাটা → নতুন নিয়ম আবিষ্কার → holdout পরীক্ষা → evolution.\nHistorical/paper research only.",reply_markup=menu())
async def cmd_status(update,context):
    if allowed(update):await send_chunks(update.effective_message,status_text())
async def cmd_logic(update,context):
    if allowed(update):
        q=qualified()
        text="🧠 QUALIFIED LOGICS\n\n"+"\n\n".join(f"{i}. {x['name']}\nHoldout {x['ha']:.1f}% n={x['hn']} | Validation {x['va']:.1f}% n={x['vn']}" for i,x in enumerate(q,1)) if q else "এখনো পর্যাপ্ত sample সহ qualified logic নেই."
        await send_chunks(update.effective_message,text)
async def cmd_report(update,context):
    if allowed(update):await send_chunks(update.effective_message,report_text())
async def cmd_discoveries(update,context):
    if allowed(update):await send_chunks(update.effective_message,discoveries_text())

async def callback(update,context):
    q=update.callback_query
    if not q:return
    try:await q.answer()
    except BadRequest as e:
        if "query is too old" not in str(e).lower() and "query id is invalid" not in str(e).lower():log.warning("callback answer: %s",e)
    except TelegramError as e:log.warning("callback answer: %s",e)
    if not allowed(update):return
    funcs={"status":status_text,"logic":lambda:cmd_logic_text(),"report":report_text,"time":time_text,"activity":activity_text,"discoveries":discoveries_text,"leaderboard":leaderboard_text,"health":health_text,"drift":drift_text,"registry":registry_text,"backtest":backtest_text}
    fn=funcs.get(q.data)
    if not fn:return
    try:
        text=await asyncio.to_thread(fn)
        if q.message:await send_chunks(q.message,text)
    except Exception:
        log.exception("callback report failed")
        if q.message:await q.message.reply_text("রিপোর্ট তৈরি করা যায়নি; hosting log দেখুন.")

def cmd_logic_text():
    q=qualified()
    return "🧠 QUALIFIED LOGICS\n\n"+"\n\n".join(f"{i}. {x['name']}\nHoldout {x['ha']:.1f}% n={x['hn']} | Validation {x['va']:.1f}% n={x['vn']}" for i,x in enumerate(q,1)) if q else "এখনো পর্যাপ্ত sample সহ qualified logic নেই."

async def collector(app):
    failures=0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                added=await asyncio.to_thread(save,normalize(await fetch(session)))
                failures=0
                if added:log.info("new periods=%s",added);event("COLLECTION",f"Inserted {added} new period(s)")
                delay=POLL_SECONDS
            except asyncio.CancelledError:raise
            except Exception as e:
                failures=min(failures+1,8)
                delay=min(60, max(POLL_SECONDS, 2 ** failures))
                log.warning("collector: %s; retry in %ss",e,delay);event("COLLECTOR_ERROR",f"{e}; retry in {delay}s")
            await asyncio.sleep(delay)

async def learner():
    global last_analyzed_issue
    while True:
        try:
            rows=await asyncio.to_thread(history)
            if len(rows)>=MIN_ROWS and rows[-1]["issue"]!=last_analyzed_issue:
                mined=await asyncio.to_thread(discover_from_training,rows)
                await asyncio.to_thread(analyze)
                made=await asyncio.to_thread(evolve)
                event("RESEARCH",f"Analyzed {len(rows)} periods; discovered {mined} data-mined rule(s), created {made} mutation(s).")
                log.info("research cycle rows=%d mined_rules=%d evolved_candidates=%d",len(rows),mined,made)
                last_analyzed_issue=rows[-1]["issue"]
        except asyncio.CancelledError:raise
        except Exception:log.exception("learner cycle failed");event("LEARNER_ERROR","See hosting logs.")
        await asyncio.sleep(LEARN_SECONDS)

async def post_init(app):
    init_db();register()
    TASKS.extend([asyncio.create_task(collector(app),name="xhawon-collector"),asyncio.create_task(learner(),name="xhawon-learner")])
    log.info("background workers started: %d",len(TASKS))
async def post_shutdown(app):
    for task in TASKS:task.cancel()
    if TASKS:await asyncio.gather(*TASKS,return_exceptions=True)
    TASKS.clear();log.info("background workers stopped")
async def error_handler(update,context):
    e=context.error
    if isinstance(e,BadRequest) and ("query is too old" in str(e).lower() or "query id is invalid" in str(e).lower()):
        log.info("expired callback ignored");return
    log.error("Unhandled Telegram update error: %s",e,exc_info=e)

def main():
    if not BOT_TOKEN:raise RuntimeError("Set BOT_TOKEN in hosting environment; do not hardcode it.")
    if not ADMIN_CHAT_ID:raise RuntimeError("Set ADMIN_CHAT_ID in hosting environment.")
    app=Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start",start));app.add_handler(CommandHandler("status",cmd_status));app.add_handler(CommandHandler("logic",cmd_logic));app.add_handler(CommandHandler("report",cmd_report));app.add_handler(CommandHandler("discover",cmd_discoveries));app.add_handler(CommandHandler("learning",cmd_report));app.add_handler(CommandHandler("health",cmd_health));app.add_handler(CommandHandler("leaderboard",cmd_leaderboard));app.add_handler(CommandHandler("backtest",cmd_backtest));app.add_handler(CommandHandler("drift",cmd_drift));app.add_handler(CommandHandler("registry",cmd_registry));app.add_handler(CommandHandler("time",cmd_time));app.add_handler(CommandHandler("export",cmd_export));app.add_handler(CommandHandler("backup",cmd_backup));app.add_handler(CommandHandler("help",cmd_help))
    app.add_handler(CallbackQueryHandler(callback));app.add_error_handler(error_handler)
    log.info("XHAWON SELF-LEARNING PATTERN AI v8.0 starting")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__":main()
