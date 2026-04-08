
import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.0/firebase-app.js";
import { getFirestore, doc, setDoc, getDoc, collection, getDocs } from "https://www.gstatic.com/firebasejs/11.6.0/firebase-firestore.js";

const firebaseConfig = {
  apiKey: "AIzaSyA1sA1BVVahQpaTGo5iE7_zzFg00-G2RFU",
  authDomain: "nurse-work.firebaseapp.com",
  projectId: "nurse-work",
  storageBucket: "nurse-work.firebasestorage.app",
  messagingSenderId: "567633609866",
  appId: "1:567633609866:web:507df36bbce06970625973"
};
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

const ADMIN_PWD = '2026N13';
const OFF_MAX = 9;
const MIN_STAFF = { D:6, E:4, N:3 };

// 2026年各月假日中線位置（假日對半分，中線畫在此日之後）
// 資料來源：中山醫學大學附設醫院115年行事曆
const HOLIDAY_MIDLINE = {
  1: 11,  // 10天假日，前5|後5，11日~17日間
  2: 16,  // 13天假日，前6|後7，16日~17日間
  3: 14,  // 9天假日，前4|後5，14日~15日間
  4: 11,  // 9天假日，前4|後5，11日~12日間
  5: 10,  // 11天假日，前5|後6，10日~16日間
  6: 14,  // 9天假日，前4|後5，14日~19日間
  7: 12,  // 8天假日，前4|後4，12日~18日間
  8: 15,  // 10天假日，前5|後5，15日~16日間
  9: 13,  // 9天假日，前4|後5，13日~19日間
  10: 11, // 10天假日，前5|後5，11日~17日間
  11: 14, // 9天假日，前4|後5，14日~15日間
  12: 13, // 9天假日，前4|後5，13日~19日間
};
const WD = ['日','一','二','三','四','五','六'];
const DEFAULT_STAFF = [
  {name:'沈佳安',shift:'D'},{name:'黃心怡',shift:'D'},{name:'熊子萱',shift:'D'},
  {name:'廖梓淳',shift:'D'},{name:'楊珮葶',shift:'D'},{name:'邱紹麒',shift:'D'},
  {name:'陳璟宜',shift:'D'},{name:'姜志胤',shift:'D'},{name:'曹育慈',shift:'D'},
  {name:'周巧蓉',shift:'EN'},{name:'王鈺婕',shift:'E'},{name:'林秉漢',shift:'E'},
  {name:'許睿庭',shift:'E'},{name:'蔡安琪',shift:'E'},{name:'張庭瑀',shift:'E'},
  {name:'黃雅亭',shift:'N'},{name:'呂伊茜',shift:'N'},{name:'朱庭儀',shift:'N'},{name:'陳安琪',shift:'N'},
];
let STAFF = [...DEFAULT_STAFF];
let latestPubShifts = {}; // 最新發布的班別（用於登入下拉）

let me = null, myDays = [], modalDay = null;
// holidayMap[y] = { "YYYY-MM-DD": { name, isHoliday, isMakeup } }
const holidayCache = {};

// ── Firebase ──
async function fbSave(name,y,m,days){ await setDoc(doc(db,'offRequests',`${name}_${y}_${m}`),{name,year:y,month:m,offDays:days,updatedAt:new Date().toISOString()}); }
async function fbLoad(name,y,m){ const s=await getDoc(doc(db,'offRequests',`${name}_${y}_${m}`)); return s.exists()?(s.data().offDays||[]): []; }
async function fbLoadAll(y,m){ const res={}; STAFF.forEach(s=>res[s.name]=[]); const snap=await getDocs(collection(db,'offRequests')); snap.forEach(d=>{const x=d.data();if(x.year===y&&x.month===m)res[x.name]=x.offDays||[];}); return res; }

// ── 政府開放假日資料 ──
// 來源：data.gov.tw 行政院人事行政總處
async function fetchHolidays(year) {
  if (holidayCache[year]) return holidayCache[year];
  const map = {};
  try {
    // 使用 data.gov.tw API
    const url = `https://data.gov.tw/api/v2/rest/datastore/472d7e3e-5e98-4a26-9061-ee0041bd1f5a?filters=year%3D${year}&limit=400&offset=0`;
    const res = await fetch(url);
    const json = await res.json();
    const records = json?.result?.records || [];
    records.forEach(r => {
      // 欄位：date, name, isHoliday
      if (!r.date) return;
      const dateStr = r.date.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3');
      const isHoliday = r.isHoliday === 'Y' || r['isHoliday'] === 'Y';
      const isMakeup = !isHoliday && r.name && r.name.includes('補班');
      if (r.name) {
        map[dateStr] = { name: r.name, isHoliday, isMakeup };
      }
    });
  } catch(e) {
    // 若抓取失敗，使用內建常見假日備援
    console.warn('假日API抓取失敗，使用備援資料', e);
    const fallback = getBuiltinHolidays(year);
    Object.assign(map, fallback);
  }
  holidayCache[year] = map;
  return map;
}

// 內建備援假日（固定假日）
function getBuiltinHolidays(year) {
  const map = {};
  // 2026年（民國115年）中山醫學大學附設醫院行事曆
  // 精確國定假日資料（H標記）
  const holidays2026 = [
    // 1月
    { m:'01', d:'01', name:'元旦開國紀念日' },
    // 2月 春節連假+和平紀念日
    { m:'02', d:'15', name:'小年夜' },
    { m:'02', d:'16', name:'農曆除夕' },
    { m:'02', d:'17', name:'春節初一' },
    { m:'02', d:'18', name:'春節初二' },
    { m:'02', d:'19', name:'春節初三' },
    { m:'02', d:'20', name:'春節' },
    { m:'02', d:'28', name:'和平紀念日' },
    // 4月 兒童節+清明
    { m:'04', d:'03', name:'兒童節補假' },
    { m:'04', d:'04', name:'兒童節/民族掃墓節' },
    { m:'04', d:'05', name:'清明節' },
    // 5月
    { m:'05', d:'01', name:'勞動節' },
    // 6月 端午節
    { m:'06', d:'19', name:'端午節' },
    // 9月 中秋節
    { m:'09', d:'25', name:'中秋節' },
    // 10月 國慶+台灣光復節
    { m:'10', d:'09', name:'國慶日補假' },
    { m:'10', d:'10', name:'國慶日' },
    { m:'10', d:'25', name:'台灣光復節' },
    // 12月
    { m:'12', d:'25', name:'行憲紀念日' },
  ];

  // 補班日（isMakeup=true）- 行事曆中的W標記
  const makeupDays2026 = [
    { m:'02', d:'20', name:'補班' }, // 2/20 補班（已含在春節內，視情況）
  ];

  if(year === 2026) {
    holidays2026.forEach(h => {
      map[`${year}-${h.m}-${h.d}`] = { name: h.name, isHoliday: true, isMakeup: false };
    });
  } else {
    // 其他年份用通用固定假日
    const fixed = [
      { m:'01', d:'01', name:'元旦' },
      { m:'02', d:'28', name:'和平紀念日' },
      { m:'04', d:'04', name:'兒童節' },
      { m:'05', d:'01', name:'勞動節' },
      { m:'10', d:'10', name:'國慶日' },
      { m:'12', d:'25', name:'行憲紀念日' },
    ];
    fixed.forEach(f => {
      map[`${year}-${f.m}-${f.d}`] = { name: f.name, isHoliday: true, isMakeup: false };
    });
  }
  return map;
}

function dateKey(y,m,d){ return `${y}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}`; }

// ── Firebase: Staff List ──
async function fbSaveStaff(staffList){
  await setDoc(doc(db,'config','staffList'),{staff:staffList,updatedAt:new Date().toISOString()});
}
async function fbLoadStaff(){
  const s=await getDoc(doc(db,'config','staffList'));
  const list=s.exists()?(s.data().staff||DEFAULT_STAFF):DEFAULT_STAFF;
  // 修正：若 Firebase 裡的 shift 是舊格式，對照 DEFAULT_STAFF 補正
  return list.map(staff=>{
    const def=DEFAULT_STAFF.find(d=>d.name===staff.name);
    // 若 Firebase 資料的 shift 是陣列（舊格式），正規化回字串
    if(Array.isArray(staff.shift)){
      const arr=staff.shift;
      const normalized=arr.includes('E')&&arr.includes('N')?'EN':arr[0]||'D';
      return {...staff,shift:normalized};
    }
    // 若 DEFAULT_STAFF 裡是 EN 但 Firebase 存的不是，修正
    if(def&&def.shift==='EN'&&staff.shift!=='EN'){
      return {...staff,shift:'EN'};
    }
    // 確保 shift 是有效值
    if(!['D','E','N','EN'].includes(staff.shift)){
      return {...staff,shift:def?.shift||'D'};
    }
    return staff;
  });
}

// ── Staff Management ──
async function loadStaffFromDB(){
  // 先用 DEFAULT_STAFF 填充（讓頁面立即可用）
  if(STAFF.length===0) { STAFF=[...DEFAULT_STAFF]; initSelect(); }
  // 同時抓最近的 publishedShifts（往回找最多3個月）
  try{
    const now=new Date(); let py=now.getFullYear(), pm=now.getMonth()+1;
    for(let i=0;i<3;i++){
      const pd=await fbLoadPublishedFull(py,pm);
      if(pd&&pd.shifts&&Object.keys(pd.shifts).length>0){
        latestPubShifts=pd.shifts; break;
      }
      pm--; if(pm<1){pm=12;py--;}
    }
  }catch(e){}
  try{
    const loaded=await fbLoadStaff();
    if(loaded&&loaded.length>0){
      STAFF=loaded;
      // 若有任何 shift 被正規化（陣列→字串），自動重存
      const needsResave=loaded.some(s=>Array.isArray(s.shift)||!['D','E','N','EN'].includes(s.shift));
      if(needsResave) fbSaveStaff(STAFF).catch(()=>{});
    }
  }catch(e){
    STAFF=[...DEFAULT_STAFF];
  }
  initSelect(); // 載入完成後更新登入選單
}

window.renderStaffTable = function renderStaffTable(){
  const tbl=document.getElementById('staffTable');
  if(!tbl) return;
  tbl.innerHTML='';
  // Header
  const hdr=document.createElement('div');
  hdr.className='staff-table-row staff-table-hdr';
  hdr.innerHTML='<div class="st-name">姓名</div><div class="st-shift">班別</div><div style="min-width:40px"></div>';
  tbl.appendChild(hdr);
  if(!STAFF.length){
    const empty=document.createElement('div');
    empty.style.cssText='padding:20px;text-align:center;color:var(--text2);font-size:13px';
    empty.textContent='尚無人員';
    tbl.appendChild(empty); return;
  }
  STAFF.forEach(s=>{
    const row=document.createElement('div'); row.className='staff-table-row';
    const shiftName=s.shift==='D'?'白班 D':s.shift==='E'?'小夜 E':s.shift==='N'?'大夜 N':s.shift==='EN'?'彈性 E+N':'大夜 N';
    const isFlex=s.shift==='FLEX'||s.shift==='EN';
    const offLabel=s.customOff!=null?`OFF:${s.customOff}天`:'OFF:9天';
    const isEN=s.shift==='EN';
    row.innerHTML=`
      <div class="st-name">${s.name}</div>
      <div class="st-shift" style="color:${isEN?'#7c3aed':s.shift==='D'?'var(--D)':s.shift==='E'?'var(--E)':'var(--N)'}">
        ${isEN?'<span style="background:#ede9fe;color:#7c3aed;border-radius:6px;padding:2px 8px;font-size:12px;font-weight:700">⚡ 彈性 E+N</span>':shiftName}
      </div>
      <button class="st-del" onclick="deleteStaffMember('${s.name}')" title="移除">✕</button>`;
    tbl.appendChild(row);
  });
}

window.toggleAddStaffForm=function(){
  const form=document.getElementById('addStaffForm');
  form.classList.toggle('show');
  if(form.classList.contains('show')) document.getElementById('newStaffName').focus();
};

window.toggleFlexOff=function(val){
  document.getElementById('flexOffGroup').style.display=val==='FLEX'?'block':'none';
};
window.addStaffMember=async function(){
  const name=document.getElementById('newStaffName').value.trim();
  const shift=document.getElementById('newStaffShift').value;
  if(!name){toast('請輸入姓名','error');return;}
  if(STAFF.find(s=>s.name===name)){toast('此姓名已存在','error');return;}
  const newMember={name,shift};
  if(shift==='FLEX'){
    const customOff=parseInt(document.getElementById('newStaffOff').value)||9;
    newMember.customOff=customOff;
  }
  STAFF.push(newMember);
  loader(true);
  try{
    await fbSaveStaff(STAFF);
    loader(false);
    document.getElementById('newStaffName').value='';
    document.getElementById('addStaffForm').classList.remove('show');
    renderStaffTable();
    initSelect();
    loadShiftAssignGrid();
    toast(`已新增 ${name} ✓`,'success');
  }catch(e){STAFF.pop();loader(false);toast('新增失敗','error');}
};

window.deleteStaffMember=async function(name){
  if(!confirm(`確定要移除「${name}」嗎？`)) return;
  const prev=[...STAFF];
  STAFF=STAFF.filter(s=>s.name!==name);
  loader(true);
  try{
    await fbSaveStaff(STAFF);
    loader(false);
    renderStaffTable();
    initSelect();
    loadShiftAssignGrid();
    toast(`已移除 ${name}`,'success');
  }catch(e){STAFF=prev;loader(false);toast('移除失敗','error');}
};

// ── Login ──
function initSelect(){
  const sel=document.getElementById('loginName');
  while(sel.options.length>1) sel.remove(1);
  STAFF.forEach(s=>{
    const o=document.createElement('option');
    o.value=s.name;
    o.textContent=s.name;
    sel.appendChild(o);
  });
}
window.doLogin=()=>{ const name=document.getElementById('loginName').value; if(!name){toast('請選擇姓名','error');return;} const s=STAFF.find(x=>x.name===name); me={name:s.name,shift:s.shift,isAdmin:false}; enterApp(); };
window.doAdminLogin=()=>{ if(document.getElementById('adminPwd').value!==ADMIN_PWD){toast('密碼錯誤','error');return;} me={name:'管理者',shift:null,isAdmin:true}; enterApp(); };
async function enterApp(){
  document.getElementById('loginScreen').classList.remove('active');
  document.getElementById('appScreen').classList.add('active');
  document.getElementById('adminTab').style.display=me.isAdmin?'block':'none';
  document.getElementById('specialRulesTab').style.display=me.isAdmin?'block':'none';
    document.getElementById('staffMgmtTab').style.display=me.isAdmin?'block':'none';
  document.getElementById('tabMySchedule').style.display=me.isAdmin?'none':'block';
  const now=new Date();
  const y=now.getFullYear(), m=now.getMonth()+1;
  ['myYear','admYear','msYear','srYear','smYear'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=y; });
  ['myMonth','admMonth','fMonth','msMonth','srMonth','smMonth'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=m; });
  _srYear=y; _srMonth=m;
  document.getElementById('fYear').value=y;
  if(!me.isAdmin){
    // Load published shift for this user
    try{
      const pub=await fbLoadPublished(y,m);
      const rawShift=pub[me.name]||me.shift;
      const assignedShift=Array.isArray(rawShift)?rawShift.join('+'):rawShift;
      const dotShift=Array.isArray(rawShift)?rawShift[0]:rawShift;
      me.displayShift=assignedShift;
      document.getElementById('userChipName').textContent=me.name;
      document.getElementById('userDot').className=`user-dot dot-${dotShift}`;
      const chip=document.getElementById('userChip');
      const isFlex=Array.isArray(rawShift);
      const shiftBg=isFlex?'linear-gradient(90deg,var(--D-bg),var(--E-bg))':dotShift==='D'?'var(--D-bg)':dotShift==='E'?'var(--E-bg)':'var(--N-bg)';
      const shiftColor=isFlex?'#92400e':dotShift==='D'?'var(--D)':dotShift==='E'?'var(--E)':'var(--N)';
      chip.innerHTML=`<div class="user-dot dot-${dotShift}"></div><span>${me.name}</span><span style="background:${shiftBg};color:${shiftColor};border-radius:6px;padding:1px 7px;font-size:11px;font-weight:700;margin-left:2px">${isFlex?'⚡'+assignedShift:assignedShift}</span>`;
    }catch(e){
      document.getElementById('userChipName').textContent=me.name;
      document.getElementById('userDot').className=`user-dot dot-${me.shift}`;
    }
    document.getElementById('tabMyOff').style.display='block';
    loadMyOff();
    loadMySchedule();
  } else {
    document.getElementById('userChipName').textContent=me.name;
    document.getElementById('userDot').className='user-dot dot-admin';
    document.getElementById('tabMyOff').style.display='none';
    switchTabByName('allReq');
    loadShiftAssignGrid();
  }
  renderAllReq();
}
window.logout=()=>{
  me=null;myDays=[];
  document.getElementById('appScreen').classList.remove('active');
  document.getElementById('loginScreen').classList.add('active');
  document.getElementById('loginName').value='';document.getElementById('adminPwd').value='';
  document.getElementById('tabMyOff').style.display='block';
  document.getElementById('tabMySchedule').style.display='none';
  document.getElementById('adminTab').style.display='none';
  document.getElementById('specialRulesTab').style.display='none';
  document.getElementById('staffMgmtTab').style.display='none';
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('tabMyOff').classList.add('active');
  document.getElementById('pageMyOff').classList.add('active');
};

// ── Tabs ──
window.switchTab=el=>{
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  el.classList.add('active');
  const n=el.dataset.page;
  document.getElementById('page'+n.charAt(0).toUpperCase()+n.slice(1)).classList.add('active');
  if(n==='allReq') renderAllReq();
  if(n==='mySchedule') loadMySchedule();
  if(n==='admin'){ loadShiftAssignGrid(); autoLoadSchedule(); }
  if(n==='specialRules') loadSpecialRulesPage();
  if(n==='staffMgmt') loadStaffMgmtPage();
};
function switchTabByName(n){const t=document.querySelector(`[data-page="${n}"]`);if(t)switchTab(t);}

// ── Calendar ──
window.loadMyOff=async()=>{
  if(!me||me.isAdmin) return;
  loader(true);
  const y=+document.getElementById('myYear').value, m=+document.getElementById('myMonth').value;
  document.getElementById('calLoading').classList.add('show');
  try{
    [myDays] = await Promise.all([fbLoad(me.name,y,m)]);
    await fetchHolidays(y); // preload
  }catch(e){toast('載入失敗','error');}
  document.getElementById('calLoading').classList.remove('show');
  loader(false);
  renderCal(y,m);
};

async function renderCal(y,m){
  const days=new Date(y,m,0).getDate(), first=new Date(y,m-1,1).getDay();
  const hmap = await fetchHolidays(y);
  document.getElementById('calTitle').textContent=`${y}年${m}月`;
  const grid=document.getElementById('calGrid'); grid.innerHTML='';
  ['日','一','二','三','四','五','六'].forEach((w,i)=>{
    const el=document.createElement('div');
    el.className='cal-wd'+(i===0||i===6?' we':'');
    el.textContent=w; grid.appendChild(el);
  });
  for(let i=0;i<first;i++){const el=document.createElement('div');el.className='cal-day empty';grid.appendChild(el);}
  for(let d=0;d<days;d++){
    const wd=(first+d)%7, we=wd===0||wd===6;
    const entry=myDays.find(x=>x.day===d);
    const key=dateKey(y,m,d+1);
    const hinfo=hmap[key];
    const isHoliday=hinfo?.isHoliday||false;
    const isMakeup=hinfo?.isMakeup||false;
    const hname=hinfo?.name||'';
    let cls='cal-day';
    if(we) cls+=' we';
    if(isHoliday) cls+=' holiday';
    if(isMakeup) cls+=' makeup';
    if(entry) cls+=' selected';
    if(entry?.note) cls+=' has-note';
    const el=document.createElement('div');
    el.className=cls;
    el.innerHTML=`<span class="day-num">${d+1}</span>${hname?`<span class="day-hname ${isHoliday?'h':isMakeup?'mk':''}">${hname}</span>`:''}`;
    el.title=hname+(entry?.note?` · ${entry.note}`:'');
    el.addEventListener('click',()=>openModal(d,y,m,hinfo));
    grid.appendChild(el);
  }
  const pill=document.getElementById('countPill');
  const realOffCount=myDays.filter(o=>!/上課|支援|公假|出勤/.test(o.note||'')).length;
  pill.textContent=`已選 ${realOffCount} / ${OFF_MAX} 天`;
  pill.className='count-pill'+(realOffCount>=OFF_MAX?' over':'');
}

// ── Modal ──
function openModal(d,y,m,hinfo){
  modalDay={d,y,m};
  const entry=myDays.find(x=>x.day===d);
  document.getElementById('noteTitle').textContent=`${y}年${m}月${d+1}日`;
  const hEl=document.getElementById('noteHoliday');
  if(hinfo?.name){
    hEl.textContent=hinfo.isHoliday?`🎌 ${hinfo.name}`:hinfo.isMakeup?`🔧 補班日：${hinfo.name}`:'';
    hEl.className='modal-holiday '+(hinfo.isHoliday?'h':'mk');
    hEl.style.display='block';
  } else { hEl.style.display='none'; }
  document.getElementById('noteInput').value=entry?.note||'';
  document.getElementById('noteModal').classList.add('show');
}
window.closeModal=()=>{document.getElementById('noteModal').classList.remove('show');modalDay=null;};
window.saveNote=async()=>{
  if(!modalDay) return;
  const {d,y,m}=modalDay, note=document.getElementById('noteInput').value.trim();
  const isNew=!myDays.find(x=>x.day===d);
  // 預假上限：超過 OFF_MAX 天直接阻擋
  if(isNew&&myDays.length>=OFF_MAX){
    toast(`預假上限 ${OFF_MAX} 天，無法繼續新增`,'error');
    closeModal();
    return;
  }
  if(isNew){
    try{
      const all=await fbLoadAll(y,m);
      const sameShift=STAFF.filter(s=>s.shift===me.shift&&s.name!==me.name);
      const offCnt=sameShift.filter(s=>(all[s.name]||[]).some(o=>o.day===d)).length;
      const working=STAFF.filter(s=>s.shift===me.shift).length-offCnt-1;
      const min=MIN_STAFF[me.shift]||3;
      const sn=me.shift==='D'?'白班':me.shift==='E'?'小夜':'大夜';
      if(working<min) toast(`⚠️ ${m}月${d+1}日 ${sn}人力可能不足（最低需${min}人）`,'error');
    }catch(e){}
  }
  const now=new Date().toISOString();
  const idx=myDays.findIndex(x=>x.day===d);
  if(idx>=0) myDays[idx]={...myDays[idx],note,updatedAt:now};
  else myDays.push({day:d,note,submittedAt:now});
  loader(true);
  try{
    await fbSave(me.name,y,m,myDays); closeModal(); loader(false);
    const msg=document.getElementById('savedMsg'); msg.classList.add('show'); setTimeout(()=>msg.classList.remove('show'),3000);
    renderCal(y,m); toast('已儲存 ✓','success');
  }catch(e){loader(false);toast('儲存失敗','error');}
};
window.removeDay=async()=>{
  if(!modalDay) return;
  const {d,y,m}=modalDay;
  myDays=myDays.filter(x=>x.day!==d);
  loader(true);
  try{await fbSave(me.name,y,m,myDays);closeModal();loader(false);renderCal(y,m);toast('已移除','success');}
  catch(e){loader(false);toast('操作失敗','error');}
};

// ── All Requests ──
window.renderAllReq=async()=>{
  const y=+document.getElementById('fYear').value, m=+document.getElementById('fMonth').value;
  const sf=document.getElementById('fShift').value;
  const list=document.getElementById('reqList');
  list.innerHTML='<div class="empty"><div class="empty-icon">⏳</div><div class="empty-text">載入中…</div></div>';
  let all; try{all=await fbLoadAll(y,m);}catch(e){list.innerHTML='<div class="empty"><div class="empty-icon">⚠️</div><div class="empty-text">載入失敗</div></div>';return;}
  const hmap=await fetchHolidays(y);
  const items=STAFF.filter(s=>!sf||s.shift===sf).map(s=>({...s,offs:all[s.name]||[]}));
  list.innerHTML='';
  if(!items.some(s=>s.offs.length)){list.innerHTML='<div class="empty"><div class="empty-icon">📭</div><div class="empty-text">本月尚無申請</div></div>';return;}
  // ── 超過 OFF 目標的警示區塊 ──
  const totalStaff=STAFF.length;
  const offGoal=Math.floor((totalStaff-13)*new Date(y,m,0).getDate()/totalStaff);
  const overLimit=items.filter(s=>{
    const realOffs=s.offs.filter(o=>!/上課|支援|公假|出勤/.test(o.note||''));
    return realOffs.length>offGoal;
  });
  if(overLimit.length>0){
    const warn=document.createElement('div');
    warn.style.cssText='background:#fef9c3;border:1.5px solid #f59e0b;border-radius:10px;padding:12px 16px;margin-bottom:14px;font-size:13px';
    warn.innerHTML=`<div style="font-weight:700;color:#92400e;margin-bottom:6px">⚠️ 以下人員預假天數超過 OFF 目標（${offGoal}天）</div>`+
      overLimit.map(s=>{
        const realOffs=s.offs.filter(o=>!/上課|支援|公假|出勤/.test(o.note||''));
        return `<div style="color:#78350f">　${s.name}：${realOffs.length} 天（超過 ${realOffs.length-offGoal} 天）</div>`;
      }).join('');
    list.appendChild(warn);
  }

  items.forEach(s=>{
    if(!s.offs.length) return;
    const sorted=[...s.offs].sort((a,b)=>a.day-b.day);
    const sameShift=STAFF.filter(x=>x.shift===s.shift);
    const tags=sorted.map(o=>{
      const offCnt=sameShift.filter(x=>(all[x.name]||[]).some(r=>r.day===o.day)).length;
      const working=sameShift.length-offCnt;
      const conflict=working<(MIN_STAFF[s.shift]||3);
      const key=dateKey(y,m,o.day+1);
      const hinfo=hmap[key];
      let extra=hinfo?.isHoliday?' 🎌':hinfo?.isMakeup?' 🔧':'';
      let cls=o.note?'noted':'';
      if(conflict) cls='conflict';
      return `<span class="day-tag ${cls}" title="${hinfo?.name||''}${o.note?' · '+o.note:''}">${o.day+1}日${extra}${o.note?' 💬':''}${conflict?' ⚠️':''}</span>`;
    }).join('');
    const notes=sorted.filter(o=>o.note).map(o=>`${o.day+1}日：${o.note}`).join('；');
    const last=sorted.reduce((a,o)=>{const t=o.updatedAt||o.submittedAt||'';return t>a?t:a;},'');
    const timeStr=last?new Date(last).toLocaleString('zh-TW',{year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}):'';
    const card=document.createElement('div'); card.className='req-card';
    card.innerHTML=`<div class="req-av av-${s.shift}">${s.shift}</div><div class="req-body"><div class="req-name">${s.name}<span class="req-shift">${s.shift==='D'?'白班':s.shift==='E'?'小夜':'大夜'}</span></div><div class="req-meta">共 ${s.offs.length} 天（上限 ${OFF_MAX} 天）</div><div class="req-tags">${tags}</div>${notes?`<div class="req-notes">💬 ${notes}</div>`:''}${timeStr?`<div class="req-time">最後更新：${timeStr}</div>`:''}</div>`;
    list.appendChild(card);
  });
};

// ── Schedule ──
window.generateSchedule=async()=>{
  const y=+document.getElementById('admYear').value;
  const m=+document.getElementById('admMonth').value;
  const dIM=new Date(y,m,0).getDate();
  document.getElementById('schedOut').innerHTML='<div class="empty"><div class="empty-icon">⚙️</div><div class="empty-text">排班中…</div></div>';
  loader(true);

  let all,hmap;
  try{[all,hmap]=await Promise.all([fbLoadAll(y,m),fetchHolidays(y)]);}
  catch(e){loader(false);toast('載入失敗','error');return;}
  loader(false);

  await loadSpecialRules(y,m);
  let publishedShifts={};
  try{const pd=await fbLoadPublishedFull(y,m);publishedShifts=pd?.shifts||{};}catch(e){}
  let staffConfig=[];
  try{staffConfig=await fbLoadStaff();}catch(e){staffConfig=[...STAFF];}

  // 上個月月底連班
  let prevStreaks={};
  try{
    const pm=m===1?12:m-1, py=m===1?y-1:y;
    const pd=await getDoc(doc(db,'monthEndStreak',`${py}_${pm}`));
    if(pd.exists()) prevStreaks=pd.data().streaks||{};
    const info=Object.entries(prevStreaks).filter(([,v])=>v>0).map(([k,v])=>`${k}:${v}天`).join('、');
    if(info) toast(`已載入上月連班：${info}`,'info');
  }catch(e){}

  // ══════════════════════════════════════
  // 規則常數
  // ══════════════════════════════════════
  const MAX_CONSEC = 6;
  const MIN_D=6, MAX_D=7;
  const MIN_E=4, MAX_E=5;
  const N_COUNT=3;
  const totalStaff=STAFF.length;
  const OFF_GOAL=Math.floor((totalStaff-13)*dIM/totalStaff);

  // ══════════════════════════════════════
  // 工具
  // ══════════════════════════════════════
  function getShift(name){
    const pub=publishedShifts[name]; if(pub) return pub;
    const s=staffConfig.find(x=>x.name===name)||STAFF.find(x=>x.name===name);
    if(!s) return 'D';
    return s.shift==='EN'?['E','N']:s.shift;
  }
  function getIA(name){
    const s=staffConfig.find(x=>x.name===name);
    return s?.inactiveAfter?(Number(s.inactiveAfter)-1):null;
  }
  function isHol(d0){
    const dt=new Date(y,m-1,d0+1),wd=dt.getDay();
    if(wd===0||wd===6) return true;
    const k=`${y}-${String(m).padStart(2,'0')}-${String(d0+1).padStart(2,'0')}`;
    return !!(hmap&&hmap[k]&&hmap[k].isHoliday);
  }

  // ══════════════════════════════════════
  // 建立每人狀態
  // ══════════════════════════════════════
  const nurses=STAFF.map(s=>{
    const raw=all[s.name]||[];
    const effShift=getShift(s.name);
    const isFlex=Array.isArray(effShift);
    const allowed=isFlex?effShift:[effShift];
    const isN=!isFlex&&allowed[0]==='N';
    const ia=getIA(s.name);

    // 預假分類
    const realOff=new Set();
    const pubOff=new Set();
    raw.forEach(o=>{
      const d=o.day; if(d<0||d>=dIM) return;
      const isPub=!!(o.note&&/上課|支援|公假|出勤/.test(o.note));
      if(isPub) pubOff.add(d); else realOff.add(d);
    });

    const iaOffs=ia!==null?Math.max(0,dIM-ia):0;
    const myGoal=isN?OFF_GOAL+1:OFF_GOAL;

    // 上個月連班
    const ps=prevStreaks[s.name]||0;
    const pl=ps>0?(isFlex?'E':allowed[0]):'OFF';

    return {
      name:s.name, shift:s.shift,
      allowed, isFlex, isN, ia,
      realOff, pubOff, myGoal, iaOffs,
      A:new Array(dIM).fill(null),
      last:pl, streak:ps,
      offCnt:0, workCnt:0,
    };
  });

  // ══════════════════════════════════════
  // 特殊規則
  // ══════════════════════════════════════
  const dayReqMap={};
  specialRules.forEach(r=>{
    if(!r.day) return;
    const d0=Number(r.day)-1; if(d0<0||d0>=dIM) return;
    const n=nurses.find(x=>x.name===r.name);
    if(r.type==='force-work'&&n){
      if(n.realOff.has(d0)) return;
      n.forceWork=n.forceWork||new Set(); n.forceWork.add(d0);
    } else if(r.type==='force-off'&&n){
      n.forceOff=n.forceOff||new Set(); n.forceOff.add(d0);
    } else if(r.type==='change-shift'&&n&&r.shift){
      if(n.realOff.has(d0)) return;
      n.tempShift=n.tempShift||{}; n.tempShift[d0]=r.shift;
    } else if(r.type==='day-req'&&r.shift){
      dayReqMap[`${d0}_${r.shift}`]={
        min:r.minCount?+r.minCount:null,
        max:r.maxCount?+r.maxCount:null
      };
    }
  });

  // ══════════════════════════════════════
  // 排班核心工具
  // ══════════════════════════════════════
  function set(n,d0,v){
    n.A[d0]=v;
    if(v==='OFF'){n.offCnt++;n.last='OFF';n.streak=0;}
    else{n.workCnt++;n.streak=n.last==='OFF'?1:n.streak+1;n.last=v;}
  }
  function getCov(d0){
    const c={D:0,E:0,N:0};
    nurses.forEach(n=>{const v=n.A[d0];if(v&&v!=='OFF')c[v]=(c[v]||0)+1;});
    return c;
  }

  // OFF 緊迫度：今天之後還需要休幾天 / 還有幾天可以休
  function urgency(n,d0){
    const realLeft=[...n.realOff].filter(d=>d>d0).length;
    const offStillNeeded=Math.max(0,n.myGoal-n.offCnt-realLeft-n.iaOffs);
    let freeLeft=0;
    for(let d=d0;d<dIM;d++){
      if(!n.realOff.has(d)&&(n.ia===null||d<n.ia)) freeLeft++;
    }
    return offStillNeeded/Math.max(1,freeLeft);
  }

  // 是否可以上這班（嚴格版，不允許連班超限）
  function canWork(n,d0,sh){
    if(n.A[d0]!==null) return false;
    if(n.realOff.has(d0)) return false;
    if(n.ia!==null&&d0>=n.ia) return false;
    if(!n.allowed.includes(sh)) return false;
    if(sh==='E'&&n.last==='N') return false;  // N→E需OFF
    if(sh==='D'&&n.last==='E') return false;  // E→D需OFF
    if(sh==='D'&&n.last==='N') return false;  // N→D需OFF
    const ns=n.last==='OFF'?1:n.streak+1;
    if(ns>MAX_CONSEC) return false;
    return true;
  }

  // ══════════════════════════════════════
  // 主排班循環
  // ══════════════════════════════════════
  for(let d0=0;d0<dIM;d0++){

    // ── Step1：強制鎖定 ──
    nurses.forEach(n=>{
      if(n.A[d0]!==null) return;
      if(n.forceOff?.has(d0)){set(n,d0,'OFF');return;}
      if(n.realOff.has(d0)){set(n,d0,'OFF');return;}
      if(n.ia!==null&&d0>=n.ia){set(n,d0,'OFF');return;}
      if(n.tempShift?.[d0]&&!n.realOff.has(d0)){set(n,d0,n.tempShift[d0]);return;}
      if(n.pubOff.has(d0)){set(n,d0,n.allowed[0]);return;}
      if(n.forceWork?.has(d0)){
        if(canWork(n,d0,n.allowed[0])) set(n,d0,n.allowed[0]);
        else set(n,d0,'OFF');
        return;
      }
    });

    // ── Step2：連班超限 → OFF ──
    nurses.forEach(n=>{
      if(n.A[d0]!==null) return;
      const ns=n.last==='OFF'?1:n.streak+1;
      if(ns>MAX_CONSEC){set(n,d0,'OFF');return;}
      if(n.last==='N'&&n.allowed.length===1&&n.allowed[0]==='E'){set(n,d0,'OFF');return;}
      // E→D 和 N→D 需要OFF間隔
      const sh0=n.allowed[0];
      if(sh0==='D'&&(n.last==='E'||n.last==='N')){set(n,d0,'OFF');return;}
    });

    // ── Step3：所有未排的人，按 urgency 排序後決定上班或OFF ──
    // OFF優先：urgency 高的人今天休，urgency 低的人今天上班
    // 同時考慮每班的上下限

    // 先強制：剩餘天數不夠補足最低OFF的人，今天必須OFF
    // 但如果人力剛好在最低標準，跳過（人力優先，後面的天補回OFF）
    nurses.forEach(n=>{
      if(n.A[d0]!==null) return;
      if(n.ia!==null&&d0>=n.ia) return;
      const remain=dIM-d0;
      const realLeft=[...n.realOff].filter(d=>d>d0).length;
      const offStillNeeded=Math.max(0,n.myGoal-n.offCnt-realLeft-n.iaOffs);
      if(remain>offStillNeeded) return; // 還來得及，不強制
      const sh=n.allowed[0];
      const req=dayReqMap[`${d0}_${sh}`]||{};
      const minT=req.min??(sh==='D'?MIN_D:sh==='E'?MIN_E:N_COUNT);
      const cv=getCov(d0);
      if((cv[sh]||0)<=minT) return; // 人力不夠，今天不強制OFF
      set(n,d0,'OFF');
    });

    const undecided=nurses.filter(n=>n.A[d0]===null);

    // 按 urgency 由低到高排（urgency低=不急休=優先上班）
    undecided.sort((a,b)=>{
      let sa=urgency(a,d0), sb=urgency(b,d0);
      // N班本班、彈性補N 優先上班（urgency加權）
      if(a.isN) sa-=0.5;
      if(b.isN) sb-=0.5;
      if(a.isFlex) sa-=0.3;
      if(b.isFlex) sb-=0.3;
      return sa-sb;
    });

    undecided.forEach(n=>{
      if(n.ia!==null&&d0>=n.ia){set(n,d0,'OFF');return;}
      const sh=n.allowed[0];
      if(!canWork(n,d0,sh)){set(n,d0,'OFF');return;}

      const req=dayReqMap[`${d0}_${sh}`]||{};
      const maxT=req.max??(sh==='D'?MAX_D:sh==='E'?MAX_E:N_COUNT);
      const minT=req.min??(sh==='D'?MIN_D:sh==='E'?MIN_E:N_COUNT);

      const cv=getCov(d0);
      const cur=cv[sh]||0;

      // 超過上限 → OFF
      if(cur>=maxT){set(n,d0,'OFF');return;}

      // urgency 決策：
      const urg=urgency(n,d0);
      const remain=dIM-d0;
      const realLeft=[...n.realOff].filter(d=>d>d0).length;
      const offStillNeeded=Math.max(0,n.myGoal-n.offCnt-realLeft-n.iaOffs);

      // 強制OFF條件1：剩餘天數不夠補足OFF → 今天必須休（保證OFF下限）
      if(remain<=offStillNeeded){
        // 但如果今天上班人力剛好在最低標準，不能休（人力優先）
        if(cur<=minT){
          // 兩難：需要休但人力不夠
          // 解法：今天上班，但把 offStillNeeded 記錄下來，系統會在之後天數補回
          set(n,d0,sh); return;
        }
        set(n,d0,'OFF'); return;
      }

      // 強制OFF條件2：urgency高 且 人數 > minT（有餘裕）→ 今天休
      // 人數必須 > minT 才允許休，確保最低人力
      if(urg>=0.5&&cur>minT){set(n,d0,'OFF');return;}

      // 人數剛好在 minT：不管urgency多高，今天上班
      if(cur<=minT){set(n,d0,sh);return;}

      // 人數在 minT~maxT 之間：看urgency決定
      if(urg>=0.7){set(n,d0,'OFF');return;}

      // 否則上班
      set(n,d0,sh);
    });
  }

  // ══════════════════════════════════════
  // 輸出
  // ══════════════════════════════════════
  const sched={};
  nurses.forEach(n=>{sched[n.name]=n.A;});

  // 統計 OFF 天數
  const offSummary=nurses.map(n=>`${n.name}:${n.offCnt}`);
  const minOff=Math.min(...nurses.filter(n=>!n.ia).map(n=>n.offCnt));
  const maxOff=Math.max(...nurses.filter(n=>!n.ia).map(n=>n.offCnt));
  toast(`排班完成 ｜ OFF目標:${OFF_GOAL}天 ｜ 實際範圍:${minOff}~${maxOff}天`,'success');

  renderSchedTable(y,m,dIM,sched,all,hmap,publishedShifts,prevStreaks);
};


function renderSchedTable(y,m,days,sched,all,hmap,pubShifts={},prevStreaks={}){
  schedState={y,m,days,sched,all,hmap,pubShifts,prevStreaks};
  const out=document.getElementById('schedOut'); out.innerHTML='';
  // 用當月發布的班別指派計算人數（更準確）
  const sc={D:0,E:0,N:0,EN:0};
  STAFF.forEach(s=>{
    const eff=pubShifts[s.name]||s.shift;
    if(eff==='EN'||( Array.isArray(eff)&&eff.includes('E')&&eff.includes('N'))) sc.EN++;
    else if(eff==='D') sc.D++;
    else if(eff==='E') sc.E++;
    else if(eff==='N') sc.N++;
  });
  const sb=document.createElement('div'); sb.className='stats-row';
  const offLimitDisplay=Math.floor((STAFF.length-13)*days/STAFF.length);
  sb.innerHTML=[{k:'D',l:'白班 D'},{k:'E',l:'小夜 E'},{k:'N',l:'大夜 N'},{k:'EN',l:'彈性班'}].map(g=>`<div class="stat-box"><div class="stat-lbl">${g.l}</div><div class="stat-val val-${g.k==='EN'?'E':g.k}">${sc[g.k]}</div></div>`).join('')+`<div class="stat-box"><div class="stat-lbl">月份天數</div><div class="stat-val">${days}</div></div><div class="stat-box"><div class="stat-lbl">OFF上限/人</div><div class="stat-val" style="color:var(--accent)">${offLimitDisplay}天</div></div><div class="stat-box"><div class="stat-lbl">最低上班人數</div><div class="stat-val" style="color:var(--ok)">13人</div></div>`;
  out.appendChild(sb);
  const hint=document.createElement('div'); hint.className='tbl-hint';
  hint.innerHTML=`🔷=預假申請　🔴=人力不足　🎌=假日　🔧=補班　← 左右滑動`;
  out.appendChild(hint);
  const wrap=document.createElement('div'); wrap.className='tbl-wrap';
  const tbl=document.createElement('table'), thead=document.createElement('thead');
  let hr='<tr><th class="col-name">姓名</th><th class="col-prev" title="上個月底班別與連續天數">上月</th>';
  for(let d=0;d<days;d++){
    const dt=new Date(y,m-1,d+1),wd=WD[dt.getDay()],we=dt.getDay()===0||dt.getDay()===6;
    const key=dateKey(y,m,d+1); const hinfo=hmap[key];
    let thCls=we?'we':'';
    if(hinfo?.isHoliday) thCls='hday';
    else if(hinfo?.isMakeup) thCls='mkday';
    const isMidline=HOLIDAY_MIDLINE[m]&&(d+1)===HOLIDAY_MIDLINE[m];
    const hmark=hinfo?.isHoliday?'🎌':hinfo?.isMakeup?'🔧':'';
    hr+=`<th class="${thCls}${isMidline?' midline-col':''}" style="${isMidline?'box-shadow:inset -3px 0 0 #ef4444':''}" title="${hinfo?.name||''}">${d+1}${hmark}<br><span style="font-size:9px;font-weight:400">${wd}</span></th>`;
  }
  hr+='<th class="col-st" style="color:#ef4444" title="國定假日/例假日的OFF天數">紅字OFF</th><th class="col-st" title="總OFF天數">總OFF</th></tr>';
  thead.innerHTML=hr; tbl.appendChild(thead);
  const tb=document.createElement('tbody');
  const cov={D:new Array(days).fill(0),E:new Array(days).fill(0),N:new Array(days).fill(0)};
  [{l:'白班 (D)',sh:'D'},{l:'小夜 (E)',sh:'E'},{l:'大夜 (N)',sh:'N'},{l:'⚡ 彈性班',sh:'FLEX'}].forEach(g=>{ // FLEX = array assignments
    const gs=STAFF.filter(s=>{
      const eff=pubShifts[s.name]||s.shift;
      if(g.sh==='FLEX') return Array.isArray(eff)||eff==='EN';
      return !Array.isArray(eff)&&eff!=='EN'&&eff===g.sh;
    }); if(!gs.length) return;
    const gr=document.createElement('tr'); gr.className='grp-row';
    // 群組標題列也加中線
    const midCol=HOLIDAY_MIDLINE[m];
    let grpHtml=`<td class="col-name" colspan="2">${g.l}</td>`;
    for(let dc=0;dc<days;dc++){
      const isMidGrp=midCol&&(dc+1)===midCol;
      grpHtml+=`<td style="${isMidGrp?'box-shadow:inset -3px 0 0 #ef4444':''}"></td>`;
    }
    grpHtml+=`<td></td><td></td>`;
    gr.innerHTML=grpHtml; tb.appendChild(gr);
    gs.forEach(s=>{
      const sc2=sched[s.name];
      const allOffs=all[s.name]||[];
      const req=allOffs.filter(o=>!/上課|支援|公假|出勤/.test(o.note||'')&&!/02/.test(o.note||'')).map(o=>o.day);
      const o2Days=new Set(allOffs.filter(o=>/02/.test(o.note||'')).map(o=>o.day));
      const pubOffDays=new Set(allOffs.filter(o=>/上課|支援|公假|出勤/.test(o.note||'')).map(o=>o.day));
      const tr=document.createElement('tr'); const ps=schedState?.prevStreaks?.[s.name]||0;
      const pl=schedState?.prevStreaks?.[s.name]!=null&&ps>0?(schedState.pubShifts?.[s.name]||s.shift):'';
      const plLabel=Array.isArray(pl)?pl[0]:pl;
      const prevCell=ps>0
        ?`<span class="prev-streak cell ${plLabel}" title="上月底連上${ps}天">${plLabel}<span class="prev-cnt">${ps}</span></span>`
        :`<span style="color:#ccc;font-size:9px">—</span>`;
      let html=`<td class="col-name">${s.name}</td><td class="col-prev">${prevCell}</td>`,wk=0,off=0;
      for(let d=0;d<days;d++){
        const v=sc2[d],dt=new Date(y,m-1,d+1),we=dt.getDay()===0||dt.getDay()===6,isR=req.includes(d);
        const key=dateKey(y,m,d+1),hinfo=hmap[key];
        let tdStyle=we?'background:#f8f9ff':'';
        if(hinfo?.isHoliday) tdStyle='background:#fff8f9';
        else if(hinfo?.isMakeup) tdStyle='background:#fffbeb';
        // O2 / 公假：算上班，但不計入人力
        const isO2=o2Days.has(d);
        const isPubOff=pubOffDays.has(d);
        const displayV=(isO2||isPubOff)?'O2':(v||'OFF');
        const isMidlineCol=HOLIDAY_MIDLINE[m]&&(d+1)===HOLIDAY_MIDLINE[m];
        const tdBorder=isMidlineCol?'box-shadow:inset -3px 0 0 #ef4444;':'';
        html+=`<td style="${tdStyle}${tdBorder}" title="${hinfo?.name||''}"><span class="cell ${displayV}${isR?' REQ':''} editable" data-name="${s.name}" data-day="${d}">${displayV}</span></td>`;
        if(displayV==='OFF') off++;
        else if(displayV==='O2') wk++; // O2：算上班但不計入人力統計
        else{wk++;if(cov[v])cov[v][d]++;}
      }
      // 計算紅字OFF（國定假日+例假日的OFF天數）
      let holOff=0;
      for(let d=0;d<days;d++){
        const v=sc2[d]||'OFF';
        if(v==='OFF'||!v){
          const dt=new Date(y,m-1,d+1),wd=dt.getDay();
          const key=dateKey(y,m,d+1),hinfo=hmap[key];
          if(wd===0||wd===6||hinfo?.isHoliday) holOff++;
        }
      }
      html+=`<td class="col-st" style="color:#ef4444;font-weight:700" title="國定假日/例假日OFF天數">${holOff}</td>`;
      html+=`<td class="col-st" style="color:#374151;font-weight:700" title="總OFF天數">${off}</td>`;
      tr.innerHTML=html;
      tb.appendChild(tr);
      // Attach click handlers to editable cells
      tr.querySelectorAll('.cell.editable').forEach(cell=>{
        const name2=cell.dataset.name; const day2=+cell.dataset.day;
        const wkEl2=tr.querySelector('.wk-cell'); const offEl2=tr.querySelector('.off-cell');
        cell.addEventListener('click',()=>openEditModal(name2,day2,cell,wkEl2,offEl2));
      });
    });
  });
  const sep=document.createElement('tr'); sep.className='cov-sep';
  sep.innerHTML=`<td colspan="${days+3}"></td>`; tb.appendChild(sep);
  ['D','E','N'].forEach(sh=>{
    if(!STAFF.filter(s=>s.shift===sh).length) return;
    const mn=MIN_STAFF[sh], cr=document.createElement('tr'); cr.className='cov-row';
    let html=`<td class="col-name">${sh}班每日人數</td><td class="col-prev"></td>`;
    for(let d=0;d<days;d++){const cnt=cov[sh][d];const isLow=cnt<mn;const isMid=HOLIDAY_MIDLINE[m]&&(d+1)===HOLIDAY_MIDLINE[m];html+=`<td style="${isMid?'box-shadow:inset -3px 0 0 #ef4444':''}"><span class="cov-num ${isLow?'cov-low':'cov-ok'} cov-cell-${sh}${isLow?' cov-clickable':''}" ${isLow?`onclick="openStaffSuggest('${sh}',${d})" title="點擊查看可補班人員"`:''}>${cnt}</span></td>`;}
    html+=`<td class="col-st">—</td><td class="col-st">—</td>`;
    cr.innerHTML=html; tb.appendChild(cr);
  });
  tbl.appendChild(tb); wrap.appendChild(tbl);

  // ── 操作列：放在班表上方（sticky 固定，隨時可見）──
  const actionBar=document.createElement('div');
  actionBar.style.cssText='display:flex;align-items:center;gap:8px;padding:10px 14px;background:var(--accent-bg,#eff6ff);border-radius:10px;font-size:12px;color:var(--accent,#2563eb);flex-wrap:wrap;margin-bottom:10px;position:sticky;top:56px;z-index:50;box-shadow:0 2px 8px rgba(0,0,0,.06)';
  actionBar.innerHTML=`
    <span style="font-size:11px">✏️ 點擊格子修改　｜　點紅色數字補班</span>
    <div style="display:flex;gap:6px;margin-left:auto;align-items:center;flex-wrap:wrap">
      <button id="undoBtn" onclick="undoEdit()" disabled style="background:#f1f5f9;color:#475569;border:1px solid #cbd5e1;border-radius:8px;padding:7px 12px;font-weight:700;cursor:pointer;font-size:12px;opacity:0.4" title="沒有可還原的操作">↩ 上一步</button>
      <button onclick="exportScheduleExcel()" style="background:#16a34a;color:#fff;border:none;border-radius:8px;padding:7px 14px;font-weight:700;cursor:pointer;font-size:12px">📊 匯出 Excel</button>
      <button onclick="saveEditedSchedule()" style="background:var(--accent,#2563eb);color:#fff;border:none;border-radius:8px;padding:7px 14px;font-weight:700;cursor:pointer;font-size:12px">💾 儲存班表</button>
    </div>`;
  // 重置 undo 歷史（新排班清空歷史）
  editHistory.length=0; updateUndoBtn();
  out.appendChild(actionBar);
  out.appendChild(wrap);
}

// ── 匯入上月班表 Excel ──
window.triggerImportExcel = function(){
  document.getElementById('importExcelInput').click();
};

window.importPrevMonthExcel = async function(input){
  const file=input.files[0];
  if(!file) return;
  input.value=''; // 重置，讓同一個檔案可以再次選

  // 動態載入 SheetJS
  if(!window.XLSX){
    await new Promise((res,rej)=>{
      const s=document.createElement('script');
      s.src='https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
      s.onload=res; s.onerror=rej; document.head.appendChild(s);
    });
  }

  loader(true);
  try{
    const buf=await file.arrayBuffer();
    const wb=XLSX.read(buf,{type:'array'});
    const wsName=wb.SheetNames[0];
    const ws=wb.Sheets[wsName];
    const rows=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});

    // ── 解析班表 ──
    // 格式：第1欄=姓名，之後每欄=每天班別（D/E/N/OFF/空白）
    // 支援我們自己匯出的格式 和 護理長原始格式（空白=上班）

    // 偵測格式：找有姓名的列
    // 找到所有護理師姓名對應的列
    const staffNames=new Set(STAFF.map(s=>s.name));
    // 也支援縮寫（育慈→曹育慈 等）
    const nameAliases={'育慈':'曹育慈','志胤':'姜志胤','小安琪':'陳安琪'};

    // 找日期列（含數字1~31的那列）
    let dateRow=-1, nameCol=-1, dayStartCol=-1;
    for(let r=0;r<Math.min(rows.length,10);r++){
      const row=rows[r];
      // 找有連續數字1,2,3...的列
      let numCount=0;
      for(let c=0;c<row.length;c++){
        const v=String(row[c]).trim();
        if(/^\d+$/.test(v)&&+v>=1&&+v<=31) numCount++;
      }
      if(numCount>=20){ // 至少有20個日期欄
        dateRow=r;
        // 找第一個數字欄（就是第1日）
        for(let c=0;c<row.length;c++){
          if(String(row[c]).trim()==='1'||+row[c]===1){
            dayStartCol=c; break;
          }
        }
        break;
      }
    }

    if(dateRow<0||dayStartCol<0){
      loader(false);
      toast('無法辨識班表格式，請確認是否為標準班表 Excel','error');
      return;
    }

    // 從日期列實際解析該月天數（避免寫死31造成多算一天）
    let maxDay=0;
    for(let c=dayStartCol;c<rows[dateRow].length;c++){
      const v=String(rows[dateRow][c]).trim();
      if(/^\d+$/.test(v)&&+v>=1&&+v<=31) maxDay=Math.max(maxDay,+v);
      else if(maxDay>0) break; // 日期欄結束了
    }
    if(maxDay===0) maxDay=31; // fallback

    // 解析每人月底連班
    const streaks={};
    let parsedCount=0;

    for(let r=dateRow+1;r<rows.length;r++){
      const row=rows[r];
      // 找姓名欄（通常在 dayStartCol 之前某欄）
      let foundName='';
      for(let c=0;c<dayStartCol;c++){
        const v=String(row[c]||'').trim();
        if(!v) continue;
        if(staffNames.has(v)){ foundName=v; break; }
        if(nameAliases[v]){ foundName=nameAliases[v]; break; }
        // 模糊比對：比對包含關係
        for(const nm of staffNames){
          if(nm.includes(v)||v.includes(nm)){ foundName=nm; break; }
        }
        if(foundName) break;
      }
      if(!foundName) continue;

      // 讀取此人每天的班別
      const days=[];
      for(let c=dayStartCol;c<dayStartCol+maxDay&&c<row.length;c++){
        const v=String(row[c]||'').trim().toUpperCase();
        // 空白=OFF（我們的格式和護理長格式統一）
        if(!v||v==='OFF') days.push('OFF');
        else if(['D','E','N'].includes(v)) days.push(v);
        else days.push('W'); // 其他值（數字、上課等）算上班
      }

      // 計算月底連班天數（從最後一天往前數）
      let streak=0;
      for(let i=days.length-1;i>=0;i--){
        if(days[i]==='OFF') break;
        streak++;
      }

      streaks[foundName]=streak;
      parsedCount++;
    }

    if(parsedCount===0){
      loader(false);
      toast('找不到護理師資料，請確認格式','error');
      return;
    }

    // 儲存到 Firebase（上個月的 monthEndStreak）
    const y=+document.getElementById('admYear').value;
    const m=+document.getElementById('admMonth').value;
    const pm=m===1?12:m-1, py=m===1?y-1:y;

    await setDoc(doc(db,'monthEndStreak',`${py}_${pm}`),{
      year:py, month:pm, streaks,
      savedAt:new Date().toISOString(),
      source:'excel_import',
    });

    loader(false);

    // 顯示結果
    const statusEl=document.getElementById('importStatus');
    const streakInfo=Object.entries(streaks)
      .filter(([,v])=>v>0)
      .map(([k,v])=>`${k}:${v}天`)
      .join('、');
    statusEl.style.display='block';
    statusEl.innerHTML=`✅ 已成功匯入上月班表（${py}年${pm}月）<br>
      共解析 <b>${parsedCount}</b> 位護理師，月底連班資訊已儲存。<br>
      ${streakInfo?`<span style="color:#15803d">有連班：${streakInfo}</span>`:'<span style="color:#15803d">所有人月底均無連班</span>'}`;

    toast(`已匯入 ${parsedCount} 位護理師的上月連班資料 ✓`,'success');

  }catch(e){
    loader(false);
    console.error(e);
    toast('匯入失敗：'+e.message,'error');
  }
};

// ── 匯出 Excel ──
window.exportScheduleExcel = async function(){
  if(!schedState){ toast('請先排班','error'); return; }
  const {y,m,days,sched,all} = schedState;
  const dIM=days;

  if(!window.ExcelJS){
    await new Promise((res,rej)=>{
      const s=document.createElement('script');
      s.src='https://cdnjs.cloudflare.com/ajax/libs/exceljs/4.3.0/exceljs.min.js';
      s.onload=res; s.onerror=rej; document.head.appendChild(s);
    });
  }

  const WD=['日','一','二','三','四','五','六'];
  const wb=new ExcelJS.Workbook();
  wb.creator='護理排班系統';
  const ws=wb.addWorksheet(`${y}.${String(m).padStart(2,'0')}`);

  // 預假 set（個人假）
  const preoffMap={};
  STAFF.forEach(s=>{
    preoffMap[s.name]=new Set((all[s.name]||[])
      .filter(o=>!/上課|支援|公假|出勤/.test(o.note||''))
      .map(o=>o.day));
  });

  // 國定假日（紅字日）
  const natHolidays=new Set();
  for(let d=0;d<dIM;d++){
    const key=`${y}-${String(m).padStart(2,'0')}-${String(d+1).padStart(2,'0')}`;
    if(schedState.hmap&&schedState.hmap[key]?.isHoliday) natHolidays.add(d);
  }

  // 假日中線欄位（0-indexed）
  const midlineDay=HOLIDAY_MIDLINE[m]; // 1-indexed
  const midlineCol=midlineDay?midlineDay-1:null; // 0-indexed

  // 顏色
  const C={
    D:'CCE5FF', E:'FFE5CC', N:'E0CCF5',
    OFF:'F2F2F2', PREOFF:'FFFF00',
    TITLE:'1F4E79', TITLE_FG:'FFFFFF',
    HDR_NORMAL:'BDD7EE', HDR_HOLIDAY:'FF0000', HDR_WEEKEND:'FFC7CE',
    HDR_FG_HOLIDAY:'FFFFFF', HDR_FG_WEEKEND:'9C0006',
    GRP_D:'DAEEF3', GRP_E:'FDE9D9', GRP_N:'EAD1DC', GRP_EN:'EDD9FF',
    STAT_LOW:'FF4444', STAT_LOW_FG:'FFFFFF', STAT_OK:'F8FAFC',
    MIDLINE:'FF0000', // 紅線顏色
  };

  // 紅線 border（右側）
  function midlineBorderRight(){
    return {style:'medium',color:{argb:'FFFF0000'}};
  }
  function addMidlineBorder(cell, colIdx){
    if(midlineCol!==null && colIdx===midlineCol){
      if(!cell.border) cell.border={};
      cell.border.right=midlineBorderRight();
    }
  }

  function fill(rgb){ return {type:'pattern',pattern:'solid',fgColor:{argb:'FF'+rgb}}; }
  function border(right=false){
    const thin={style:'thin',color:{argb:'FFCCCCCC'}};
    const b={top:thin,bottom:thin,left:thin,right:thin};
    if(right) b.right={style:'medium',color:{argb:'FFFF0000'}};
    return b;
  }
  function font(sz=8,bold=false,color='000000'){
    return {name:'Arial',size:sz,bold,color:{argb:'FF'+color}};
  }
  function align(h='center'){return {horizontal:h,vertical:'middle'};}

  // 欄寬
  ws.getColumn(1).width=7;   // 姓名
  ws.getColumn(2).width=4;   // 上月
  for(let d=3;d<=dIM+2;d++) ws.getColumn(d).width=2.6;
  ws.getColumn(dIM+3).width=5;  // 紅字OFF
  ws.getColumn(dIM+4).width=5;  // 總OFF

  // ── 標題列 ──
  ws.getRow(1).height=18;
  const titleCell=ws.getCell(1,1);
  titleCell.value=`${y}年${m}月護理排班表`;
  titleCell.fill=fill(C.TITLE);
  titleCell.font=font(11,true,C.TITLE_FG);
  titleCell.alignment=align('left');
  ws.mergeCells(1,1,1,dIM+4);

  // ── 日期列 ──
  ws.getRow(2).height=15;
  // 姓名
  const nameHdr=ws.getCell(2,1);
  nameHdr.value='姓名'; nameHdr.fill=fill(C.HDR_NORMAL);
  nameHdr.font=font(8,true); nameHdr.alignment=align();
  nameHdr.border=border();
  // 上月
  const prevHdr=ws.getCell(2,2);
  prevHdr.value='上月'; prevHdr.fill=fill(C.HDR_NORMAL);
  prevHdr.font=font(8,true); prevHdr.alignment=align();
  prevHdr.border=border();

  for(let d=0;d<dIM;d++){
    const dt=new Date(y,m-1,d+1);
    const wd=dt.getDay();
    const isWe=wd===0||wd===6;
    const isHol=natHolidays.has(d);
    const isMid=midlineCol!==null&&d===midlineCol;
    const cell=ws.getCell(2,d+3);
    cell.value=`${d+1}${WD[wd]}`;
    // 紅字日=紅底白字，週末=粉底紅字，一般=藍底
    if(isHol){
      cell.fill=fill(C.HDR_HOLIDAY);
      cell.font=font(7,true,C.TITLE_FG);
    } else if(isWe){
      cell.fill=fill(C.HDR_WEEKEND);
      cell.font=font(7,true,C.HDR_FG_WEEKEND);
    } else {
      cell.fill=fill(C.HDR_NORMAL);
      cell.font=font(7,true);
    }
    cell.alignment=align();
    cell.border=border(isMid);
  }
  [['紅字OFF','ef4444'],['總OFF','374151']].forEach(([v,clr],i)=>{
    const c=ws.getCell(2,dIM+3+i);
    c.value=v; c.fill=fill(C.HDR_NORMAL);
    c.font=font(8,true,clr); c.alignment=align(); c.border=border();
  });

  let curRow=3;

  // ── 各班 ──
  const groups=[
    {label:'白班 (D)',shift:'D',grp:'GRP_D'},
    {label:'小夜 (E)',shift:'E',grp:'GRP_E'},
    {label:'大夜 (N)',shift:'N',grp:'GRP_N'},
    {label:'彈性班',shift:'EN',grp:'GRP_EN'},
  ];

  groups.forEach(g=>{
    const gs=STAFF.filter(s=>{
      const eff=schedState.pubShifts?.[s.name]||s.shift;
      if(g.shift==='EN') return Array.isArray(eff)||eff==='EN';
      return !Array.isArray(eff)&&eff!=='EN'&&eff===g.shift;
    });
    if(!gs.length) return;

    // 群組標題列
    ws.getRow(curRow).height=12;
    const grpCell=ws.getCell(curRow,1);
    grpCell.value=g.label;
    grpCell.fill=fill(C[g.grp]);
    grpCell.font=font(8,true);
    grpCell.alignment=align('left');
    grpCell.border=border();
    ws.mergeCells(curRow,1,curRow,2);
    // 群組標題每個日期格也要有中線
    for(let d=0;d<dIM;d++){
      const isMid=midlineCol!==null&&d===midlineCol;
      const gc=ws.getCell(curRow,d+3);
      gc.fill=fill(C[g.grp]);
      gc.border=border(isMid);
    }
    ws.getCell(curRow,dIM+3).fill=fill(C[g.grp]);
    ws.getCell(curRow,dIM+4).fill=fill(C[g.grp]);
    curRow++;

    // 每個人
    gs.forEach(s=>{
      ws.getRow(curRow).height=14;
      const r=sched[s.name]||[];
      const preoffs=preoffMap[s.name];

      // 姓名
      const nameCell=ws.getCell(curRow,1);
      nameCell.value=s.name;
      nameCell.font=font(8,false);
      nameCell.alignment=align('left');
      nameCell.border=border();

      // 上月連班（從 schedState.prevStreaks）
      const ps=schedState.prevStreaks?.[s.name]||0;
      const prevCell=ws.getCell(curRow,2);
      prevCell.value=ps>0?`${ps}天`:'';
      prevCell.font=font(7,false,'666666');
      prevCell.alignment=align();
      prevCell.border=border();

      let offCnt=0, workCnt=0;
      for(let d=0;d<dIM;d++){
        const v=r[d]||'OFF';
        const isPreoff=preoffs.has(d);
        const isHol=natHolidays.has(d);
        const isMid=midlineCol!==null&&d===midlineCol;
        const cell=ws.getCell(curRow,d+3);
        cell.value=v;
        cell.border=border(isMid);
        cell.alignment=align();
        if(isPreoff){
          // 預假：黃色
          cell.fill=fill(C.PREOFF);
          cell.font=font(8,true,'856404');
        } else if(v==='D'){
          cell.fill=fill(isHol?'99C4E4':C.D);
          cell.font=font(8,true,isHol?'1F4E79':'000000');
        } else if(v==='E'){
          cell.fill=fill(isHol?'F5C57B':C.E);
          cell.font=font(8,true,isHol?'7B3F00':'000000');
        } else if(v==='N'){
          cell.fill=fill(isHol?'C9A8E8':C.N);
          cell.font=font(8,true,isHol?'4B0082':'000000');
        } else {
          cell.fill=fill(C.OFF);
          cell.font=font(7,false,'888888');
        }
        if(v==='OFF') offCnt++; else workCnt++;
      }

      // 計算紅字OFF（國定假日+例假日OFF天數）
      let holOffXlsx=0;
      for(let d=0;d<dIM;d++){
        const v=r[d]||'OFF';
        if(v==='OFF'){
          const dt=new Date(y,m-1,d+1),wd=dt.getDay();
          if(wd===0||wd===6||natHolidays.has(d)) holOffXlsx++;
        }
      }
      const holOffCell=ws.getCell(curRow,dIM+3);
      holOffCell.value=holOffXlsx; holOffCell.font=font(8,true,'ef4444');
      holOffCell.alignment=align(); holOffCell.border=border();
      const totalOffCell=ws.getCell(curRow,dIM+4);
      totalOffCell.value=offCnt; totalOffCell.font=font(8,true,'374151');
      totalOffCell.alignment=align(); totalOffCell.border=border();

      curRow++;
    });
  });

  // ── 每日人數統計 ──
  curRow++;
  ['D','E','N'].forEach(sh=>{
    ws.getRow(curRow).height=13;
    const minT=sh==='D'?6:sh==='E'?4:3;
    const grpKey=sh==='D'?'GRP_D':sh==='E'?'GRP_E':'GRP_N';
    const lblCell=ws.getCell(curRow,1);
    lblCell.value=`${sh}班人數`;
    lblCell.fill=fill(C[grpKey]);
    lblCell.font=font(8,true);
    lblCell.alignment=align('left');
    lblCell.border=border();
    ws.mergeCells(curRow,1,curRow,2);

    for(let d=0;d<dIM;d++){
      const cnt=STAFF.filter(s=>(sched[s.name]||[])[d]===sh).length;
      const isMid=midlineCol!==null&&d===midlineCol;
      const isLow=cnt<minT;
      const cell=ws.getCell(curRow,d+3);
      cell.value=cnt;
      cell.fill=fill(isLow?C.STAT_LOW:C.STAT_OK);
      cell.font=font(8,true,isLow?C.STAT_LOW_FG:'333333');
      cell.alignment=align();
      cell.border=border(isMid);
    }
    ws.getCell(curRow,dIM+3).border=border();
    ws.getCell(curRow,dIM+4).border=border();
    curRow++;
  });

  // 下載
  const buf=await wb.xlsx.writeBuffer();
  const blob=new Blob([buf],{type:'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url; a.download=`${y}年${m}月護理排班表.xlsx`; a.click();
  URL.revokeObjectURL(url);
  toast('Excel 匯出完成 ✓','success');
};

// ── Firebase: Published Shifts ──
async function fbSavePublished(y,m,shifts){
  await setDoc(doc(db,'publishedShifts',`${y}_${m}`),{year:y,month:m,shifts,publishedAt:new Date().toISOString()});
}
async function fbLoadPublished(y,m){
  const s=await getDoc(doc(db,'publishedShifts',`${y}_${m}`));
  return s.exists()?(s.data().shifts||{}):{};
}
async function fbLoadPublishedFull(y,m){
  const s=await getDoc(doc(db,'publishedShifts',`${y}_${m}`));
  return s.exists()?s.data():null;
}

// ── Firebase: Published Schedule (班表) ──
async function fbSaveSchedule(y,m,scheduleData){
  await setDoc(doc(db,'publishedSchedule',`${y}_${m}`),{year:y,month:m,schedule:scheduleData,publishedAt:new Date().toISOString()});
}
async function fbLoadSchedule(y,m){
  const s=await getDoc(doc(db,'publishedSchedule',`${y}_${m}`));
  return s.exists()?s.data():null;
}

// ── Shift Assignment Grid (Admin) ──
let pendingShifts = {}; // name -> 'D'|'E'|'N'

window.loadShiftAssignGridForMonth = async function(y, m){
  const grid=document.getElementById('shiftAssignGrid');
  if(!grid) return;
  await _loadShiftGrid(y, m);
};

window.loadShiftAssignGrid = async function loadShiftAssignGrid(){
  renderStaffTable();
  const y=+document.getElementById('smYear')?.value||+document.getElementById('admYear').value;
  const m=+document.getElementById('smMonth')?.value||+document.getElementById('admMonth').value;
  await _loadShiftGrid(y, m);
};

async function _loadShiftGrid(y, m){
  const grid=document.getElementById('shiftAssignGrid');
  grid.innerHTML='<div style="padding:20px;color:var(--text2);font-size:13px">⏳ 載入中…</div>';
  let pub={}, pubData=null;
  try{ pubData=await fbLoadPublishedFull(y,m); pub=pubData?.shifts||{}; }catch(e){}
  // Init pendingShifts from published or STAFF default
  pendingShifts={};
  STAFF.forEach(s=>{ pendingShifts[s.name]=pub[s.name]||s.shift; }); // pub[name] may be array for flex
  // Status
  const statusEl=document.getElementById('publishStatus');
  if(pubData){
    const t=new Date(pubData.publishedAt).toLocaleString('zh-TW',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
    statusEl.innerHTML=`<span class="published-badge">✓ 已發布 ${t}</span>`;
  } else {
    statusEl.innerHTML='<span style="color:var(--text2)">尚未發布</span>';
  }
  // Render rows grouped by shift
  grid.innerHTML='';
  // 單一清單渲染（支援跨群組拖曳排序）
  grid.innerHTML='';
  let dragSrc=null;
  STAFF.forEach(s=>{
      const row=document.createElement('div');
      row.className='shift-row'; row.dataset.name=s.name;
      row.draggable=true;
      const defaultShift=s.shift==='EN'?['E','N']:s.shift;
      const cur=pendingShifts[s.name]||defaultShift;
      const curArr=Array.isArray(cur)?cur:[cur];
      const isFlex=curArr.length>1;
      const shiftLabel=isFlex?curArr.join('+')+(` <span class="flex-badge-small">⚡ 彈性</span>`):
        (curArr[0]==='D'?'<span style="color:var(--D);font-weight:700">白班 D</span>':
         curArr[0]==='E'?'<span style="color:var(--E);font-weight:700">小夜 E</span>':
                        '<span style="color:var(--N);font-weight:700">大夜 N</span>');
      const shiftColor=curArr[0]==='D'?'#3b82f6':curArr[0]==='E'?'#f59e0b':curArr[0]==='N'?'#8b5cf6':'#10b981';
      row.style.borderLeft=`3px solid ${shiftColor}`;
      row.innerHTML=`
        <div style="display:flex;align-items:center;gap:8px;min-width:0">
          <span class="drag-handle" style="cursor:grab;color:#94a3b8;font-size:16px;padding:0 4px;user-select:none" title="拖曳排序">☰</span>
          <div class="shift-name">${s.name}</div>
        </div>
        <div class="shift-current-wrap">
          <div class="shift-current">${shiftLabel}</div>
          ${isFlex?'':'<div class="flex-hint">可多選→彈性班</div>'}
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <div class="shift-btns" data-name="${s.name}">
            <button class="shift-btn ${curArr.includes('D')?'active-D':''}" onclick="toggleShiftBtn('${s.name}','D',this.parentNode)">D</button>
            <button class="shift-btn ${curArr.includes('E')?'active-E':''}" onclick="toggleShiftBtn('${s.name}','E',this.parentNode)">E</button>
            <button class="shift-btn ${curArr.includes('N')?'active-N':''}" onclick="toggleShiftBtn('${s.name}','N',this.parentNode)">N</button>
          </div>
          <button onclick="removeFromGrid('${s.name}')" style="background:#fee2e2;color:#b91c1c;border:none;border-radius:6px;padding:4px 8px;cursor:pointer;font-size:11px;font-weight:700">✕</button>
        </div>`;
      // 拖曳排序事件（支援跨班別）
      row.addEventListener('dragstart',e=>{
        dragSrc=s.name;
        e.dataTransfer.effectAllowed='move';
        setTimeout(()=>row.style.opacity='0.4',0);
      });
      row.addEventListener('dragend',()=>{ row.style.opacity='1'; dragSrc=null; });
      row.addEventListener('dragover',e=>{
        e.preventDefault(); e.dataTransfer.dropEffect='move';
        row.style.outline='2px dashed #3b82f6';
      });
      row.addEventListener('dragleave',()=>{ row.style.outline=''; });
      row.addEventListener('drop',e=>{
        e.preventDefault(); row.style.outline='';
        if(!dragSrc||dragSrc===s.name) return;
        const fromIdx=STAFF.findIndex(x=>x.name===dragSrc);
        const toIdx=STAFF.findIndex(x=>x.name===s.name);
        if(fromIdx<0||toIdx<0) return;
        const [moved]=STAFF.splice(fromIdx,1);
        STAFF.splice(toIdx,0,moved);
        fbSaveStaff(STAFF);
        _loadShiftGrid(y,m);
      });
      grid.appendChild(row);
  });
}

// 新增人員到班別指派（臨時，不影響 STAFF 永久名單）
window.showAddToGridModal = function(){
  // 建立輸入表單在 warnModal 裡
  document.getElementById('warnModalIcon').textContent='👤';
  document.getElementById('warnModalTitle').textContent='新增護理師';
  document.getElementById('warnModalMsg').innerHTML=`
    <div style="text-align:left;margin-bottom:10px">
      <label style="font-size:12px;color:#64748b;display:block;margin-bottom:4px">姓名</label>
      <input id="newStaffNameInput" type="text" placeholder="請輸入姓名" maxlength="10"
        style="width:100%;padding:10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:14px;box-sizing:border-box"
        onkeydown="if(event.key==='Enter')document.getElementById('confirmAddStaff').click()">
    </div>
    <div style="text-align:left">
      <label style="font-size:12px;color:#64748b;display:block;margin-bottom:4px">預設班別</label>
      <select id="newStaffShiftInput"
        style="width:100%;padding:10px;border:1.5px solid #e2e8f0;border-radius:8px;font-size:14px">
        <option value="D">D 白班</option>
        <option value="E">E 小夜</option>
        <option value="N">N 大夜</option>
        <option value="EN">⚡ 彈性 E+N</option>
      </select>
    </div>`;
  document.getElementById('warnModalBtns').innerHTML=`
    <button id="confirmAddStaff" onclick="doAddStaff()"
      style="padding:12px;border-radius:10px;border:none;font-size:14px;font-weight:600;cursor:pointer;width:100%;background:#2563eb;color:#fff">
      確認新增
    </button>
    <button onclick="document.getElementById('warnModal').style.display='none'"
      style="padding:12px;border-radius:10px;border:none;font-size:14px;font-weight:600;cursor:pointer;width:100%;background:#f1f5f9;color:#374151">
      取消
    </button>`;
  window._warnBtns=[];
  document.getElementById('warnModal').style.display='flex';
  setTimeout(()=>document.getElementById('newStaffNameInput')?.focus(),100);
};

window.doAddStaff = function(){
  const name=document.getElementById('newStaffNameInput')?.value.trim();
  const shift=document.getElementById('newStaffShiftInput')?.value||'D';
  if(!name){toast('請輸入姓名','error');return;}
  if(STAFF.find(s=>s.name===name)){
    toast(`${name} 已在名單中`,'error');return;
  }
  STAFF.push({name,shift});
  pendingShifts[name]=shift==='EN'?['E','N']:shift;
  fbSaveStaff(STAFF);
  initSelect(); // 更新登入下拉選單
  document.getElementById('warnModal').style.display='none';
  const y=+document.getElementById('smYear')?.value||new Date().getFullYear();
  const m=+document.getElementById('smMonth')?.value||new Date().getMonth()+1;
  _loadShiftGrid(y,m);
  toast(`已新增 ${name}（${shift}班），登入選單已更新`,'success');
};

window.removeFromGrid = function(name){
  showWarnModal({
    icon:'⚠️',
    title:`移除 ${name}？`,
    msg:`確定要將「${name}」從人員名單移除嗎？
移除後登入選單也會同步更新。`,
    buttons:[
      {label:'確認移除', danger:true, onClick:()=>{
        STAFF=STAFF.filter(s=>s.name!==name);
        delete pendingShifts[name];
        fbSaveStaff(STAFF);
        initSelect(); // 更新登入下拉選單
        const y=+document.getElementById('smYear')?.value||new Date().getFullYear();
        const m=+document.getElementById('smMonth')?.value||new Date().getMonth()+1;
        _loadShiftGrid(y,m);
        toast(`已移除 ${name}，登入選單已更新`,'success');
      }},
      {label:'取消'}
    ]
  });
};

window.toggleShiftBtn=function(name, shift, btnsEl){
  // Get current selection as array
  let cur=pendingShifts[name]||name; // fallback
  const curArr=Array.isArray(cur)?[...cur]:[cur];
  const idx=curArr.indexOf(shift);
  if(idx>=0){
    // Deselect - but must have at least 1 selected
    if(curArr.length<=1){toast('至少需選擇一個班別','error');return;}
    curArr.splice(idx,1);
  } else {
    // Select - max 2 shifts for flex
    if(curArr.length>=2){toast('最多選擇兩個班別','error');return;}
    curArr.push(shift);
    curArr.sort();
  }
  pendingShifts[name]=curArr.length===1?curArr[0]:curArr;
  // Update buttons
  btnsEl.querySelectorAll('.shift-btn').forEach(b=>{
    const sh=b.textContent.charAt(0); // 'D','E','N'
    b.className=`shift-btn${curArr.includes(sh)?' active-'+sh:''}`;
  });
  // Update current label
  const row=btnsEl.closest('.shift-row');
  const curEl=row.querySelector('.shift-current');
  const isFlex=curArr.length>1;
  const label=isFlex?
    curArr.join('+')+'<span class="flex-badge-small" style="margin-left:6px">⚡ 彈性</span>':
    (curArr[0]==='D'?'<span style="color:var(--D);font-weight:700">白班 D</span>':
     curArr[0]==='E'?'<span style="color:var(--E);font-weight:700">小夜 E</span>':
                    '<span style="color:var(--N);font-weight:700">大夜 N</span>');
  curEl.innerHTML=label;
  const hintEl=row.querySelector('.flex-hint');
  if(hintEl) hintEl.style.display=isFlex?'none':'block';
};

window.publishShifts=async function(){
  const y=+document.getElementById('smYear')?.value||+document.getElementById('admYear').value;
  const m=+document.getElementById('smMonth')?.value||+document.getElementById('admMonth').value;
  if(!Object.keys(pendingShifts).length){toast('請先設定班別','error');return;}
  loader(true);
  try{
    await fbSavePublished(y,m,pendingShifts);
    loader(false);
    // 同步更新 STAFF 的預設 shift（讓登入下拉顯示正確班別）
    STAFF=STAFF.map(s=>{
      const ps=pendingShifts[s.name];
      if(!ps) return s;
      // 陣列 ['E','N'] → 'EN'；單一 'D'/'E'/'N' → 直接用
      const newShift=Array.isArray(ps)?(ps.includes('E')&&ps.includes('N')?'EN':ps[0]):ps;
      return {...s,shift:newShift};
    });
    await fbSaveStaff(STAFF); // 存回 Firebase
    latestPubShifts=pendingShifts; // 更新最新發布班別
    initSelect(); // 更新登入下拉
    toast(`${y}年${m}月班別已發布 ✓`,'success');
    const _y=+document.getElementById('smYear')?.value||+document.getElementById('admYear').value;
    const _m=+document.getElementById('smMonth')?.value||+document.getElementById('admMonth').value;
    _loadShiftGrid(_y,_m); // refresh status
  }catch(e){loader(false);toast('發布失敗，請重試','error');}
};

// ── My Schedule Page ──
window.loadMySchedule=async function(){
  if(!me||me.isAdmin) return;
  const y=+document.getElementById('msYear').value, m=+document.getElementById('msMonth').value;
  const out=document.getElementById('myScheduleOut');
  out.innerHTML='<div class="empty"><div class="empty-icon">⏳</div><div class="empty-text">載入中…</div></div>';
  try{
    const [pubData, hmap, myOffs] = await Promise.all([
      fbLoadPublishedFull(y,m),
      fetchHolidays(y),
      fbLoad(me.name,y,m)
    ]);
    if(!pubData){
      out.innerHTML=`<div class="ms-unpublished"><div style="font-size:32px;margin-bottom:8px">🔒</div><div style="font-weight:700;font-size:15px;margin-bottom:4px">班表尚未發布</div><div style="font-size:13px">護理長發布後即可查看</div></div>`;
      return;
    }
    const myShift=pubData.shifts[me.name]||me.shift;
    const pubTime=new Date(pubData.publishedAt).toLocaleString('zh-TW',{year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
    const dIM=new Date(y,m,0).getDate();
    const first=new Date(y,m-1,1).getDay();
    const offDays=myOffs.map(o=>o.day);
    // Build calendar
    out.innerHTML=`
      <div style="background:var(--white);border-radius:14px;box-shadow:var(--shadow);padding:20px;">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;flex-wrap:wrap;gap:8px;">
          <div style="font-size:17px;font-weight:700;">${y}年${m}月 班表</div>
          <div style="display:flex;align-items:center;gap:8px;">
            <span class="published-badge">✓ 護理長已發布</span>
            <span style="background:${myShift==='D'?'var(--D-bg)':myShift==='E'?'var(--E-bg)':'var(--N-bg)'};color:${myShift==='D'?'var(--D)':myShift==='E'?'var(--E)':'var(--N)'};border-radius:8px;padding:4px 12px;font-size:13px;font-weight:700;">我的班別：${myShift==='D'?'白班D':myShift==='E'?'小夜E':'大夜N'}</span>
          </div>
        </div>
        <div style="font-size:11px;color:var(--text2);margin-bottom:14px;">發布時間：${pubTime}　🔵=預假日</div>
        <div class="ms-grid" id="msGrid"></div>
        <div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:4px;font-size:11px;color:var(--text2);">
          <span>🟢 D 白班</span><span>🟡 E 小夜</span><span>🟣 N 大夜</span><span>⬜ OFF 休假</span><span>🔵 點=預假</span>
        </div>
      </div>`;
    const grid=document.getElementById('msGrid');
    // Weekday headers
    ['日','一','二','三','四','五','六'].forEach((w,i)=>{
      const el=document.createElement('div');
      el.className='ms-wd'+(i===0||i===6?' we':'');
      el.textContent=w; grid.appendChild(el);
    });
    // Empty cells
    for(let i=0;i<first;i++){const el=document.createElement('div');el.className='ms-day empty';grid.appendChild(el);}
    // Days
    for(let d=0;d<dIM;d++){
      const wd=(first+d)%7, we=wd===0||wd===6;
      const key=dateKey(y,m,d+1), hinfo=hmap[key];
      const isOff=offDays.includes(d);
      const shiftVal=isOff?'OFF':myShift;
      let cls='ms-day';
      if(we) cls+=' we';
      if(hinfo?.isHoliday) cls+=' holiday';
      else if(hinfo?.isMakeup) cls+=' makeup';
      const el=document.createElement('div'); el.className=cls;
      el.innerHTML=`<span class="ms-dnum">${d+1}</span><span class="ms-shift ${shiftVal}">${shiftVal}</span>${isOff?'<span class="ms-off-dot"></span>':''}${hinfo?.isHoliday?`<span class="ms-hname">${hinfo.name}</span>`:''}`;
      grid.appendChild(el);
    }
  }catch(e){
    out.innerHTML='<div class="empty"><div class="empty-icon">⚠️</div><div class="empty-text">載入失敗，請重試</div></div>';
  }
};

// ── 手動編輯班表 ──
let schedState = null; // { y, m, days, sched, all, hmap, pubShifts }
let editTarget = null; // { name, day0, cellEl, wkEl, offEl }
let editHistory = []; // undo history (max 50 steps)

// ── 連班檢查工具 ──
function calcStreakAfterEdit(name, day0, newVal){
  if(!schedState) return {warnings:[]};
  const A=[...schedState.sched[name]];
  A[day0]=newVal;
  const prevStreaks=schedState.prevStreaks||{};
  const ps=prevStreaks[name]||0;
  const s=STAFF.find(x=>x.name===name);
  const eff=schedState.pubShifts?.[name]||s?.shift||'D';
  const pl=ps>0?(eff==='EN'?'E':Array.isArray(eff)?eff[0]:eff):'OFF';
  let last=pl, streak=ps;
  const streaks=[];
  for(let d=0;d<A.length;d++){
    const av2=A[d]==='O2'?'D':A[d]; // O2 算上班（當D計算連班）
    if(!av2||av2==='OFF'){last='OFF';streak=0;}
    else{streak=last==='OFF'?1:streak+1; last=av2;}
    streaks.push(streak);
  }
  const warnings=[];
  for(let d=0;d<A.length;d++){
    if(streaks[d]>6) warnings.push({type:'consec',day:d,streak:streaks[d]});
  }
  for(let d=1;d<A.length;d++){
    if(A[d]==='E'&&A[d-1]==='N') warnings.push({type:'shift',day:d,from:'N',to:'E',msg:'N後不能直接排E'});
    if(A[d]==='D'&&A[d-1]==='E') warnings.push({type:'shift',day:d,from:'E',to:'D',msg:'E後不能直接排D'});
    if(A[d]==='D'&&A[d-1]==='N') warnings.push({type:'shift',day:d,from:'N',to:'D',msg:'N後不能直接排D'});
  }
  return {streaks, warnings, streakAtDay:streaks[day0]};
}

window.openEditModal = function(name, day0, cellEl, wkEl, offEl){
  editTarget = {name, day0, cellEl, wkEl, offEl};
  const dt = new Date(schedState.y, schedState.m-1, day0+1);
  document.getElementById('editModalTitle').textContent =
    `${name}  ${schedState.m}月${day0+1}日（${['日','一','二','三','四','五','六'][dt.getDay()]}）`;

  // 不在選項旁顯示提示，改為點下去後才跳出警示
  const infoEl=document.getElementById('editOptionsInfo');
  if(infoEl) infoEl.innerHTML='';

  document.getElementById('editModal').classList.add('show');
};
// ── Undo 功能 ──
function updateUndoBtn(){
  const btn=document.getElementById('undoBtn');
  if(!btn) return;
  btn.disabled=editHistory.length===0;
  btn.title=editHistory.length>0?`返回上一步（${editHistory.length}步可還原）`:'沒有可還原的操作';
  btn.style.opacity=editHistory.length===0?'0.4':'1';
}

window.undoEdit = function(){
  if(!schedState||editHistory.length===0) return;
  const {name, day0, oldVal} = editHistory.pop();
  schedState.sched[name][day0] = oldVal;
  // 更新顯示
  const cellEl=document.querySelector(`.cell.editable[data-name="${name}"][data-day="${day0}"]`);
  if(cellEl){
    cellEl.className=`cell ${oldVal} editable`;
    cellEl.textContent=oldVal;
  }
  // 更新上班/OFF統計
  const r=schedState.sched[name];
  const wk=r.filter(v=>v&&v!=='OFF').length;
  const off=r.filter(v=>v==='OFF').length;
  const tr=cellEl?.closest('tr');
  if(tr){
    const wkEl=tr.querySelector('.col-wk');
    const offEl=tr.querySelector('.col-off');
    if(wkEl) wkEl.textContent=wk;
    if(offEl) offEl.textContent=off;
  }
  refreshCovRow(schedState);
  updateUndoBtn();
  toast(`已還原：${name} ${schedState.m}/${day0+1} → ${oldVal}`,'info');
};

window.closeEditModal = function(){
  document.getElementById('editModal').classList.remove('show');
  editTarget = null;
};
window.applyEdit = function(newVal){
  if(!editTarget || !schedState) return;
  const {name, day0, cellEl, wkEl, offEl} = editTarget;

  if(newVal !== 'OFF'){
    const result = calcStreakAfterEdit(name, day0, newVal);
    const warns = result?.warnings||[];
    const shiftWarn = warns.find(w=>w.type==='shift');
    const consecWarn = warns.find(w=>w.type==='consec');

    if(shiftWarn){
      // 班別切換違規：完全阻止
      closeEditModal();
      showWarnModal({
        icon:'❌',
        title:'無法修改',
        msg:`${name} ${schedState.m}/${shiftWarn.day+1} ${shiftWarn.msg}，中間需要安排 OFF 才能切換班別。`,
        buttons:[{label:'確定', primary:true, onClick:()=>{}}]
      });
      return;
    }
    if(consecWarn){
      // 連班超限：詢問確認
      closeEditModal();
      showWarnModal({
        icon:'⚠️',
        title:'連班天數警告',
        msg:`${name} 改為 ${newVal} 後，${schedState.m}/${consecWarn.day+1} 會連班 ${consecWarn.streak} 天（超過 6 天上限）。
確定要修改嗎？`,
        buttons:[
          {label:'確定修改', primary:true, onClick:()=>_doApplyEdit(name,day0,newVal,cellEl,wkEl,offEl)},
          {label:'取消'}
        ]
      });
      return;
    }
  }
  _doApplyEdit(name, day0, newVal, cellEl, wkEl, offEl);
};

function _doApplyEdit(name, day0, newVal, cellEl, wkEl, offEl){
  const old = schedState.sched[name][day0];
  editHistory.push({name, day0, oldVal:old, newVal});
  if(editHistory.length>50) editHistory.shift();
  updateUndoBtn();
  schedState.sched[name][day0] = newVal;
  cellEl.className = `cell ${newVal} editable`;
  cellEl.textContent = newVal;
  const r = schedState.sched[name];
  const wk = r.filter(v=>v&&v!=='OFF').length;
  const off = r.filter(v=>v==='OFF').length;
  if(wkEl) wkEl.textContent = wk;
  if(offEl) offEl.textContent = off;
  refreshCovRow(schedState);
  toast(`${name} ${schedState.m}/${day0+1} 改為 ${newVal}`,'success');
};
function refreshCovRow(st){
  if(!st) return;
  const {sched, days} = st;
  const cov={D:new Array(days).fill(0),E:new Array(days).fill(0),N:new Array(days).fill(0)};
  STAFF.forEach(s=>{
    const r=sched[s.name];
    if(!r) return;
    for(let d=0;d<days;d++) if(r[d]&&r[d]!=='OFF') cov[r[d]][d]=(cov[r[d]][d]||0)+1;
  });
  ['D','E','N'].forEach(sh=>{
    const mn=MIN_STAFF[sh]||3;
    const cells=document.querySelectorAll(`.cov-cell-${sh}`);
    cells.forEach((c,i)=>{
      const cnt=cov[sh][i];
      c.textContent=cnt;
      c.className=`cov-num ${cnt<mn?'cov-low':'cov-ok'} cov-cell-${sh}`;
    });
  });
}

// ── 補班建議 ──
window.openStaffSuggest = function(sh, day0){
  if(!schedState) return;
  const {y,m,sched,prevStreaks={}} = schedState;
  const mn = sh==='D'?6:sh==='E'?4:3;
  const cur = STAFF.filter(s=>(sched[s.name]||[])[day0]===sh).length;
  const need = mn-cur;

  // 找今天 OFF 的人，哪些可以出來補
  const candidates=[];
  STAFF.forEach(s=>{
    const r=sched[s.name]||[];
    if(r[day0]!=='OFF') return; // 今天已上班，跳過

    // 預假/公假不能動
    const preoffs=schedState.all?.[s.name]||[];
    const isRealOff=preoffs.some(o=>o.day===day0&&!/上課|支援|公假|出勤/.test(o.note||''));
    const isPubOff=preoffs.some(o=>o.day===day0&&/上課|支援|公假|出勤/.test(o.note||''));
    if(isRealOff) return; // 預假鎖定，不可補班

    // 確認班別符合
    const eff=schedState.pubShifts?.[s.name]||s.shift;
    const allowed=eff==='EN'?['E','N']:(Array.isArray(eff)?eff:[eff]);
    if(!allowed.includes(sh)) return;

    // 確認連班不超限
    const ps=prevStreaks[s.name]||0;
    const effS=eff==='EN'?'E':(Array.isArray(eff)?eff[0]:eff);
    const pl=ps>0?effS:'OFF';
    let last=pl, streak=ps;
    for(let d=0;d<day0;d++){
      const v=r[d];
      if(!v||v==='OFF'){last='OFF';streak=0;}
      else{streak=last==='OFF'?1:streak+1;last=v;}
    }
    const nsAfter=last==='OFF'?1:streak+1;
    const consecOk=nsAfter<=6;
    const shiftOk=!(sh==='E'&&last==='N')&&!(sh==='D'&&last==='E')&&!(sh==='D'&&last==='N');

    // 計算此人的OFF天數
    const offCnt=r.filter(v=>!v||v==='OFF').length;
    // 計算還剩幾天及OFF緊迫度
    const totalStaff=STAFF.length;
    const offGoal=Math.floor((totalStaff-13)*(schedState.days||30)/totalStaff)+(s.shift==='N'?1:0);

    candidates.push({
      name:s.name, offCnt, offGoal,
      consecOk, neOk, streak:nsAfter,
      canWork:consecOk&&shiftOk,
      isPubOff, // 公假（算上班，但班別已固定）
    });
  });

  // 排序：可補班的人優先，再按OFF天數由多到少
  candidates.sort((a,b)=>{
    if(a.canWork!==b.canWork) return a.canWork?-1:1;
    return b.offCnt-a.offCnt; // OFF多的先出來
  });

  // 渲染
  const dt=new Date(y,m-1,day0+1);
  const WD=['日','一','二','三','四','五','六'];
  document.getElementById('suggestTitle').textContent=
    `${m}/${day0+1}（${WD[dt.getDay()]}）${sh}班人力不足`;
  document.getElementById('suggestSub').textContent=
    `目前 ${cur} 人，最少需要 ${mn} 人，還差 ${need} 人。點擊人員直接補班。`;

  const list=document.getElementById('suggestList');
  if(!candidates.length){
    list.innerHTML='<div class="suggest-empty">今天沒有可補班的人員<br>（所有人今天不是預假就是連班超限）</div>';
  } else {
    const canList=candidates.filter(c=>c.canWork);
    const cantList=candidates.filter(c=>!c.canWork);
    let html='';
    if(canList.length){
      html+=`<div style="font-size:11px;font-weight:700;color:#16a34a;margin-bottom:6px">✓ 可以出來補班（${canList.length}人）</div>`;
      canList.forEach(c=>{
        const streakTxt=c.streak>0?`連班${c.streak}天`:'';
        const offDiff=c.offCnt-c.offGoal;
        const offTxt=offDiff>0?`OFF多${offDiff}天`:offDiff<0?`OFF少${Math.abs(offDiff)}天`:'OFF剛好';
        html+=`<div class="suggest-person" onclick="applySuggest('${c.name}',${day0},'${sh}')">
          <span class="cell ${sh}" style="flex-shrink:0">${sh}</span>
          <span class="suggest-person-name">${c.name}</span>
          <span class="suggest-person-streak" style="background:${offDiff>=0?'#dcfce7':'#fef9c3'};color:${offDiff>=0?'#166534':'#854d0e'}">${offTxt}</span>
          ${streakTxt?`<span class="suggest-person-info">${streakTxt}</span>`:''}
        </div>`;
      });
    }
    if(cantList.length){
      html+=`<div style="font-size:11px;font-weight:700;color:#94a3b8;margin:10px 0 6px">✗ 無法補班（${cantList.length}人）</div>`;
      cantList.forEach(c=>{
        const reason=!c.consecOk?`連班會達${c.streak}天`:!c.shiftOk?'班別切換需要OFF間隔':'';
        html+=`<div class="suggest-person" style="opacity:0.5;cursor:default">
          <span class="cell ${sh}" style="flex-shrink:0;opacity:0.4">${sh}</span>
          <span class="suggest-person-name">${c.name}</span>
          <span class="suggest-person-warn">${reason}</span>
        </div>`;
      });
    }
    list.innerHTML=html;
  }
  document.getElementById('suggestModal').classList.add('show');
};

window.applySuggest = function(name, day0, sh){
  if(!schedState) return;
  const old=schedState.sched[name][day0];
  // 記錄歷史
  editHistory.push({name,day0,oldVal:old,newVal:sh});
  if(editHistory.length>50) editHistory.shift();
  updateUndoBtn();
  // 修改
  schedState.sched[name][day0]=sh;
  // 更新格子
  const cellEl=document.querySelector(`.cell.editable[data-name="${name}"][data-day="${day0}"]`);
  if(cellEl){cellEl.className=`cell ${sh} editable`;cellEl.textContent=sh;}
  // 更新統計
  const r=schedState.sched[name];
  const wk=r.filter(v=>v&&v!=='OFF').length;
  const off=r.filter(v=>v==='OFF').length;
  const tr=cellEl?.closest('tr');
  if(tr){
    const wkEl=tr.querySelector('.col-wk');
    const offEl=tr.querySelector('.col-off');
    if(wkEl) wkEl.textContent=wk;
    if(offEl) offEl.textContent=off;
  }
  refreshCovRow(schedState);
  document.getElementById('suggestModal').classList.remove('show');
  toast(`${name} ${schedState.m}/${day0+1} 補排 ${sh} 班 ✓`,'success');
};

// ── 自動載入班表（排班管理頁進入時）──
async function autoLoadSchedule(){
  const y=+document.getElementById('admYear').value;
  const m=+document.getElementById('admMonth').value;
  // 如果已有 schedState（已排班），不重複載入
  if(schedState&&schedState.y===y&&schedState.m===m) return;
  try{
    const saved=await fbLoadSchedule(y,m);
    if(!saved||!saved.schedule) return;
    const sched=saved.schedule;
    const dIM=new Date(y,m,0).getDate();
    const [all,hmap]=await Promise.all([fbLoadAll(y,m),fetchHolidays(y)]);
    let publishedShifts={};
    try{const pd=await fbLoadPublishedFull(y,m);publishedShifts=pd?.shifts||{};}catch(e){}
    let prevStreaks={};
    try{
      const pm=m===1?12:m-1,py=m===1?y-1:y;
      const pd=await getDoc(doc(db,'monthEndStreak',`${py}_${pm}`));
      if(pd.exists()) prevStreaks=pd.data().streaks||{};
    }catch(e){}
    renderSchedTable(y,m,dIM,sched,all,hmap,publishedShifts,prevStreaks);
    toast(`已自動載入 ${y}年${m}月 儲存的班表`,'info');
  }catch(e){}
}

// ── 自訂警告 Modal ──
window._warnBtns = [];
function showWarnModal({icon='⚠️', title='', msg='', buttons=[]}){
  document.getElementById('warnModalIcon').textContent=icon;
  document.getElementById('warnModalTitle').textContent=title;
  document.getElementById('warnModalMsg').textContent=msg;
  const btnsEl=document.getElementById('warnModalBtns');
  btnsEl.innerHTML=buttons.map((b,i)=>`
    <button onclick="handleWarnBtn(${i})" style="padding:12px;border-radius:10px;border:${b.danger?'1.5px solid #ef4444':'none'};font-size:14px;font-weight:600;cursor:pointer;width:100%;
      background:${b.primary?'#2563eb':b.danger?'#fff':'#f1f5f9'};
      color:${b.primary?'#fff':b.danger?'#ef4444':'#374151'}">
      ${b.label}
    </button>`).join('');
  window._warnBtns=buttons;
  document.getElementById('warnModal').style.display='flex';
}
window.handleWarnBtn=function(i){
  document.getElementById('warnModal').style.display='none';
  const btn=window._warnBtns?.[i];
  if(btn?.onClick) btn.onClick();
};

// ── Helpers ──
function loader(v){document.getElementById('loader').style.display=v?'flex':'none';}
let toastTmr;
function toast(msg,type='success'){const t=document.getElementById('toast');t.textContent=msg;t.className=`toast ${type} show`;clearTimeout(toastTmr);toastTmr=setTimeout(()=>t.classList.remove('show'),3000);}

// ── 計算月底連班天數（儲存到 Firebase 供下個月使用）──
function calcMonthEndStreak(sched, dIM){
  // 回傳每人月底連續上班天數（空白=OFF）
  const streaks={};
  STAFF.forEach(s=>{
    const nm=s.name;
    const r=sched[nm]||[];
    let count=0;
    for(let d=dIM-1;d>=0;d--){
      const v=r[d];
      if(!v||v==='OFF') break;
      count++;
    }
    streaks[nm]=count;
  });
  return streaks;
}

// ── 特殊規則管理 ──
let specialRules = []; // [{id, type, name, day, shift, minCount, maxCount, note}]
let _srYear = null, _srMonth = null;

// 顯示特殊規則頁面時同步年月
window.loadStaffMgmtPage = async function(){
  const y=+document.getElementById('smYear')?.value||new Date().getFullYear();
  const m=+document.getElementById('smMonth')?.value||new Date().getMonth()+1;
  loadShiftAssignGridForMonth(y, m);
};

window.loadSpecialRulesPage = async function(){
  const y = +document.getElementById('srYear').value;
  const m = +document.getElementById('srMonth').value;
  _srYear = y; _srMonth = m;
  await loadSpecialRules(y, m);
  renderSpecialRulesPage();
};

async function loadSpecialRules(y, m){
  try{
    const d = await getDoc(doc(db,'specialRules',`${y}_${m}`));
    specialRules = d.exists() ? (d.data().rules||[]) : [];
    if(specialRules.length) toast(`已載入 ${specialRules.length} 條特殊規則`,'info');
  }catch(e){ specialRules=[]; }
}

window.addSpecialRule = function(){
  const y = _srYear||+document.getElementById('srYear')?.value||+document.getElementById('admYear').value;
  const m = _srMonth||+document.getElementById('srMonth')?.value||+document.getElementById('admMonth').value;
  _srYear=y; _srMonth=m;
  specialRules.push({
    id: Date.now(),
    type:'force-work', name: STAFF[0]?.name||'',
    day:'1', shift:'D', minCount:'', maxCount:'', note:''
  });
  renderSpecialRulesPage();
};

window.deleteRule = function(id){
  specialRules = specialRules.filter(r=>r.id!==id);
  renderSpecialRulesPage();
};

window.updateRule = function(id, key, val){
  const r = specialRules.find(x=>x.id===id);
  if(r) r[key]=val;
  renderSpecialRulesPage();
};

function renderSpecialRulesPage(){
  const list = document.getElementById('srRulesList');
  if(!list) return;
  const y = _srYear||+document.getElementById('srYear')?.value||2026;
  const m = _srMonth||+document.getElementById('srMonth')?.value||4;
  const dIM = new Date(y, m, 0).getDate();

  if(!specialRules.length){
    list.innerHTML = `<div style="text-align:center;padding:32px;color:#94a3b8;font-size:14px">
      尚無特殊規則<br><span style="font-size:12px">點「＋ 新增規則」開始設定</span></div>`;
    return;
  }

  const TYPE_META = {
    'force-work': {label:'🟢 強制上班', cls:'sr-type-force-work'},
    'force-off':  {label:'🔴 強制OFF',  cls:'sr-type-force-off'},
    'change-shift':{label:'🟡 改換班別', cls:'sr-type-change-shift'},
    'day-req':    {label:'🟣 當日人力', cls:'sr-type-day-req'},
  };

  list.innerHTML = specialRules.map(r => {
    const meta = TYPE_META[r.type]||TYPE_META['force-work'];
    const typeOpts = Object.entries(TYPE_META).map(([v,{label}])=>
      `<option value="${v}" ${r.type===v?'selected':''}>${label}</option>`).join('');
    const nameOpts = STAFF.map(s=>
      `<option value="${s.name}" ${r.name===s.name?'selected':''}>${s.name}</option>`).join('');
    const dayOpts = Array.from({length:dIM},(_,i)=>
      `<option value="${i+1}" ${r.day==i+1?'selected':''}>${i+1}日</option>`).join('');
    const shiftOpts = ['D','E','N'].map(s=>
      `<option value="${s}" ${r.shift===s?'selected':''}>${s}班</option>`).join('');

    const isDayReq = r.type==='day-req';
    const isChangeShift = r.type==='change-shift';
    const needsName = !isDayReq;

    return `<div class="sr-rule-card sr-type-border-${r.type}" style="border-left:4px solid ${
      r.type==='force-work'?'#10b981':r.type==='force-off'?'#ef4444':r.type==='change-shift'?'#f59e0b':'#8b5cf6'}">
      <div class="sr-rule-header">
        <span class="sr-type-badge ${meta.cls}">${meta.label}</span>
        <select onchange="updateRule(${r.id},'type',this.value)">${typeOpts}</select>
        ${needsName?`<select onchange="updateRule(${r.id},'name',this.value)">
          <option value="">選擇人員</option>${nameOpts}</select>`:''}
        <select onchange="updateRule(${r.id},'day',this.value)">
          <option value="">選擇日期</option>${dayOpts}</select>
        ${isDayReq?`
          <select onchange="updateRule(${r.id},'shift',this.value)">${shiftOpts}</select>
          <input type="number" min="0" max="20" placeholder="最少" value="${r.minCount||''}"
            style="width:60px" onchange="updateRule(${r.id},'minCount',this.value)">
          <span style="color:#94a3b8;font-size:12px">～</span>
          <input type="number" min="0" max="20" placeholder="最多" value="${r.maxCount||''}"
            style="width:60px" onchange="updateRule(${r.id},'maxCount',this.value)">`
        :isChangeShift?`
          <select onchange="updateRule(${r.id},'shift',this.value)">
            <option value="">改為</option>${shiftOpts}</select>`
        :''}
        <button class="sr-del-btn" onclick="deleteRule(${r.id})">✕ 刪除</button>
      </div>
      <textarea class="sr-rule-note" rows="1" placeholder="備註說明（可選，如：石珮玉支援大夜）"
        onchange="updateRule(${r.id},'note',this.value)">${r.note||''}</textarea>
    </div>`;
  }).join('');
}

window.saveSpecialRules = async function(){
  const y = _srYear||+document.getElementById('srYear')?.value||+document.getElementById('admYear').value;
  const m = _srMonth||+document.getElementById('srMonth')?.value||+document.getElementById('admMonth').value;
  loader(true);
  try{
    await setDoc(doc(db,'specialRules',`${y}_${m}`),{
      year:y, month:m, rules:specialRules, savedAt:new Date().toISOString()
    });
    loader(false);
    toast(`${y}年${m}月特殊規則已儲存（共${specialRules.length}條）✓`,'success');
  }catch(e){ loader(false); toast('儲存失敗','error'); }
};

// ── 儲存已調整的班表 ──// ── 儲存已調整的班表 ──
window.saveEditedSchedule = async function(){
  if(!schedState) return;
  const {y,m,sched} = schedState;
  const dIM=new Date(y,m,0).getDate();
  loader(true);
  try{
    // 儲存班表
    await fbSaveSchedule(y,m,sched);
    // 同時計算並儲存月底連班資料（供下個月排班使用）
    const streaks=calcMonthEndStreak(sched,dIM);
    await setDoc(doc(db,'monthEndStreak',`${y}_${m}`),{
      year:y, month:m, streaks,
      savedAt:new Date().toISOString()
    });
    loader(false);
    toast('班表已儲存 ✓（已記錄月底連班資料）','success');
  }catch(e){
    loader(false);
    toast('儲存失敗','error');
  }
};

// ── Init ──
initSelect(); // 立即填充（用 DEFAULT_STAFF）
loadStaffFromDB(); // 從 Firebase 更新
document.getElementById('noteModal').addEventListener('click',function(e){if(e.target===this)closeModal();});
