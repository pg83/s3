(()=>{
'use strict';
let data={hosts:[],cells:[],partitions:[],buckets:[]};
const $=id=>document.getElementById(id), $$=s=>[...document.querySelectorAll(s)];
const n=(v,d=1)=>v==null||!Number.isFinite(v)?'—':new Intl.NumberFormat('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}).format(v);
const bytes=(v,d=1)=>v==null?'—':Math.abs(v)>=1e12?n(v/1e12,d)+' TB':Math.abs(v)>=1e9?n(v/1e9,d)+' GB':Math.abs(v)>=1e6?n(v/1e6,d)+' MB':Math.abs(v)>=1e3?n(v/1e3,d)+' kB':n(v,0)+' B';
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time=t=>new Date(t*1000).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',timeZone:'UTC'});
const growth=v=>v==null?'—':(v>0?'+':'')+bytes(v*3600,0);
const scaled=(v,k)=>v==null?null:v*k;
const percent=c=>c.used!=null&&c.capacity>0?c.used/c.capacity*100:null;
let hours=24,filter='all',selected=null,lastFocus=null;
let items=[],byKey=new Map();
function indexData(){
 items=[...data.cells.map(c=>({...c,key:'hdd-'+c.id,role:'hdd',free:c.capacity!=null&&c.used!=null?c.capacity-c.used:null,activity:c.read_rps!=null&&c.write_rps!=null?c.read_rps+c.write_rps:null,capacity_label:'Used space',activity_label:'Cell RPS'})),...data.partitions.map(c=>({...c,key:c.id}))];
 byKey=new Map(items.map(c=>[c.key,c]));
}
const warn=c=>c.stale||c.up!==1||c.smart.scrape!==1||c.smart.health===0||(c.role==='hdd'&&(c.smart.pending>0||c.smart.uncorrectable>0))||(c.role==='etcd'&&(c.has_leader!==1||c.free/c.capacity<.1))||(c.role==='store'&&c.free/c.capacity<.1);
const points=(c,kind)=>c[kind==='capacity'?'capacity_history':'rps_history'].filter(p=>p[0]>=data.timestamp-hours*3600&&Number.isFinite(p[1]));
function state(c){
 if(c.stale)return 'Stale metrics';
 if(c.up!==1)return 'Unavailable';
 if(c.smart.scrape!==1)return 'SMART unknown';
 if(c.role==='hdd')return c.up!==1?'Offline':c.smart.health===0?'SMART failed':c.smart.pending>0?n(c.smart.pending,0)+' pending':c.smart.uncorrectable>0?'SMART warning':'Available';
 if(c.smart.health===0)return 'SMART failed';
 if(c.role==='etcd')return c.has_leader!==1?'No leader':c.free/c.capacity<.1?'Low space':c.is_leader===1?'Leader':'Follower';
 if(c.role==='load')return 'LRU cache';
 return c.free/c.capacity<.1?'Low space':'Ready';
}
function chartPath(c,kind,w,h,top=1){
 const values=points(c,kind);if(values.length<2)return null;
 const ys=values.map(p=>p[1]),low=kind==='capacity'?Math.min(...ys):0,high=Math.max(...ys),span=high-low;
 const xy=values.map(p=>[(p[0]-(data.timestamp-hours*3600))/(hours*3600)*w,span?h-2-(p[1]-low)/span*(h-top-3):(h+top)/2]);
 return {values,low,high,line:xy.map((p,i)=>(i?'L':'M')+p.map(v=>v.toFixed(2)).join(',')).join(' '),first:xy[0][0],last:xy.at(-1)[0]};
}
function mini(c){
 const cap=chartPath(c,'capacity',300,23),act=chartPath(c,'activity',300,23);
 const capName=c.role==='hdd'?'Used':c.role==='etcd'?'DB':c.role==='load'?'Cached':'Queued';
 return `<div class="mini-history" title="${esc(c.capacity_label)} and ${esc(c.activity_label)}, ${hours}h. Each line has its own vertical scale; click for values."><div class="mini-legend"><span><i></i>${capName}</span><span class="activity"><i></i>${esc(c.activity_label)}</span></div><svg viewBox="0 0 300 23" preserveAspectRatio="none" aria-hidden="true">${cap?`<path d="${cap.line} L${cap.last},23 L${cap.first},23 Z" fill="var(--muted)" opacity=".08"/><path d="${cap.line}" stroke="var(--muted)" stroke-width="1" fill="none" vector-effect="non-scaling-stroke"/>`:''}${act?`<path d="${act.line}" stroke="var(--accent)" stroke-width="1.15" fill="none" vector-effect="non-scaling-stroke"/>`:''}</svg></div>`;
}
function stat(label,value,title=''){return `<div class="stat" title="${esc(title)}"><label>${label}</label><strong>${value}</strong></div>`;}
function tile(c){
 const w=warn(c),pct=percent(c),formatted=bytes(c.used).split(' '),label=c.role==='hdd'?'HDD '+c.index+' · #'+String(c.id).padStart(2,'0'):c.label;
 let stats,note;
 if(c.role==='hdd'){
  stats=stat('Fill / h',growth(c.growth),'Physical S3 allocation growth; 1h linear trend')+stat('Cell RPS',n(c.activity,2),'Read + append operations, 5m average; not disk IOPS')+stat('Errors / 24h',n(c.errors,0),'S3 cell failures and rejected operations');
  note=w?`${n(c.smart.reallocated,0)} realloc · ${n(c.smart.uncorrectable,0)} uncorrectable`:`↓ ${n(c.read_rps,2)} / ↑ ${n(c.write_rps,2)} RPS`;
 }else if(c.role==='etcd'){
  stats=stat('Fsync p99',n(scaled(c.fsync_p99,1000),1)+'<span class="unit">ms</span>','WAL fsync 99th percentile, last 5 minutes')+stat('RPC/s',n(c.activity,1),'Completed unary gRPC calls; 5m average')+stat('Failed / 24h',n(c.errors,0),'Failed Raft proposals, not all possible etcd errors');
  note=`DB ${bytes(c.db_size)} <span class="subtle">/ ${bytes(c.quota)} quota</span>`;
 }else{
  stats=stat((c.role==='load'?'Cache':'Queue')+' Δ / h',growth(c.growth),'Payload change, 1h linear trend. Does not include filesystem overhead.')+stat('Disk IOPS',n(c.iops,1),'Reads + writes for this partition from /proc/diskstats, sampled over '+n(c.io_sample_seconds,0)+' seconds')+stat('Errors / 24h','—','Per-partition error counts are not currently exported');
  note=c.role==='load'?'LRU · space reused on demand':'Write buffer → HDD '+c.index;
 }
 const tail=c.role==='hdd'?(c.smart.pending_delta!=null?'Δ pending '+(c.smart.pending_delta>=0?'+':'')+n(c.smart.pending_delta,0):''):c.role==='etcd'?n(c.iops,1)+' IOPS':'cell '+String(c.cell_id).padStart(2,'0');
 return `<article class="cell${w?' warning':''}" tabindex="0" role="button" data-key="${esc(c.key)}" data-role="${c.role}" aria-label="${esc(c.host+' '+label+' '+state(c)+', '+bytes(c.free)+' free')}"><div class="cell-top"><span class="device">/dev/${esc(c.device)}<span class="cell-id">${esc(label)}</span></span><span class="state${w?' warn':''}"><i class="dot${w?' warn':''}"></i>${state(c)}</span></div><div class="model">${esc(c.smart.model)}</div><div class="capacity-line"><span class="used-number">${formatted[0]}</span><span class="used-unit">${formatted[1]||''}</span><span class="capacity-total">/ ${bytes(c.capacity,1)}</span><span class="percent">${n(pct)}%</span></div><div class="capacity-bar"><span style="width:${Math.min(100,pct||0)}%"></span></div><div class="space-line"><span><span class="free">${bytes(c.free,2)}</span> free</span><span>${n(c.smart.temperature,0)}°C</span></div><div class="stats">${stats}</div>${mini(c)}<div class="tile-note${w?' warn':''}"><span>${note}</span><span>${tail}</span></div></article>`;
}
function renderSummary(){
 const available=fields=>data.cells.filter(c=>c.up===1&&!c.stale&&fields.every(field=>Number.isFinite(c[field])));
 const capacityCells=available(['used','capacity']),requestCells=available(['read_rps','write_rps']),growthCells=available(['growth']);
 const sum=(cells,field)=>cells.length?cells.reduce((total,c)=>total+c[field],0):null;
 const coverage=cells=>{
  if(cells.length===data.cells.length)return '';
  const missing=data.cells.filter(c=>!cells.includes(c)).map(c=>c.host+' HDD '+c.index).join(', ');
  return ` <span class="summary-coverage" title="${esc('Partial total; missing metrics: '+missing)}">${cells.length}/${data.cells.length} cells</span>`;
 };
 const used=sum(capacityCells,'used'),cap=sum(capacityCells,'capacity'),reads=sum(requestCells,'read_rps'),writes=sum(requestCells,'write_rps'),fill=scaled(sum(growthCells,'growth'),3600);
 $('summary').innerHTML=`<div class="summary-item"><div class="eyebrow">Used capacity${coverage(capacityCells)}</div><div class="summary-number">${n(scaled(used,1e-12),2)}<span class="unit">TB <span style="color:var(--dim)">/ ${n(scaled(cap,1e-12),2)} TB</span></span></div><div class="micro-track"><span style="width:${used!=null&&cap>0?used/cap*100:0}%"></span></div></div><div class="summary-item"><div class="eyebrow">Free space${coverage(capacityCells)}</div><div class="summary-number">${n(used!=null&&cap!=null?(cap-used)/1e12:null,2)}<span class="unit">TB</span></div><div class="summary-note">${n(used!=null&&cap>0?(cap-used)/cap*100:null)}% available</div></div><div class="summary-item"><div class="eyebrow">Cell requests${coverage(requestCells)}</div><div class="summary-number">${n(reads!=null&&writes!=null?reads+writes:null)}<span class="unit">RPS</span></div><div class="summary-note">${n(reads)} read <span class="subtle">/</span> ${n(writes)} append</div></div><div class="summary-item"><div class="eyebrow">Fill rate${coverage(growthCells)}</div><div class="summary-number">${fill>0?'+':''}${n(scaled(fill,1e-9),2)}<span class="unit">GB/h</span></div><div class="summary-note">trend over the last hour</div></div>`;
}
function render(){
 let html='';
 data.hosts.forEach((h,i)=>{const live=data.cells.filter(c=>c.host===h.name).every(c=>c.up===1&&!c.stale);html+=`<div class="host"><div class="host-name"><i class="dot${live?'':' warn'}"></i>${esc(h.name)}</div>${i===data.hosts.length-1?'<div class="segment" aria-label="Chart time range">'+[1,6,24].map(v=>`<button data-range="${v}" class="${v===hours?'active':''}" aria-pressed="${v===hours}">${v}h</button>`).join('')+'</div>':''}</div>`;});
 const indices=[...new Set(data.cells.map(c=>c.index))].sort((a,b)=>a-b);
 const rows=[...indices.map(index=>({role:'hdd',index})),{role:'etcd',index:null},...indices.map(index=>[{role:'store',index},{role:'load',index}]).flat()];
 rows.forEach(row=>data.hosts.forEach(h=>{const c=items.find(c=>c.host===h.name&&c.role===row.role&&c.index===row.index);html+=c?tile(c):'<div></div>';}));
 $('matrix').style.gridTemplateColumns=`repeat(${Math.max(1,data.hosts.length)},minmax(250px,1fr))`;
 $('matrix').innerHTML=html;
 $$('[data-key]').forEach(el=>{el.onclick=()=>openDrawer(el.dataset.key);el.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();openDrawer(el.dataset.key);}};});
 $$('[data-range]').forEach(b=>b.onclick=()=>{hours=+b.dataset.range;render();if(selected)renderDrawer();document.querySelector(`[data-range="${hours}"]`).focus({preventScroll:true});});
 applyFilter();
}
function applyFilter(){
 $$('[data-filter]').forEach(b=>{const active=b.dataset.filter===filter;b.classList.toggle('selected',active);b.setAttribute('aria-pressed',active);b.querySelector('.count').textContent=items.filter(c=>b.dataset.filter==='all'||(b.dataset.filter==='attention'?warn(c):!warn(c))).length;});
 $$('[data-key]').forEach(el=>{const w=warn(byKey.get(el.dataset.key));el.classList.toggle('filtered-out',filter==='attention'?!w:filter==='healthy'?w:false);});
}
function largeChart(c,kind){
 const path=chartPath(c,kind,380,125,26);if(!path)return '<div class="detail-note">No samples in this time range.</div>';
 const format=v=>kind==='capacity'?bytes(v,2):n(v,2)+' '+c.activity_label;
 return `<div class="chart-panel${kind==='capacity'?' plot-capacity':''}" data-chart="${kind}"><svg class="large-chart" style="color:inherit" viewBox="0 0 384 125" preserveAspectRatio="none" aria-label="${esc(kind==='capacity'?c.capacity_label:c.activity_label)}"><path class="chart-grid" d="M0 25H384M0 75H384M0 124H384"/><text x="0" y="12" class="chart-scale">${format(path.high)}</text><path d="${path.line} L${path.last},125 L${path.first},125 Z" fill="currentColor" opacity=".06"/><path d="${path.line}" fill="none" stroke="currentColor" stroke-width="1.4" vector-effect="non-scaling-stroke"/></svg><div class="chart-tooltip" hidden></div><div class="chart-axis"><span>${time(data.timestamp-hours*3600)}</span><span>${time(data.timestamp-hours*1800)}</span><span>${time(data.timestamp)} UTC</span></div></div>`;
}
const row=(label,value)=>`<tr><td>${label}</td><td>${value}</td></tr>`;
const table=rows=>'<table class="detail-table"><tbody>'+rows+'</tbody></table>';
function renderDrawer(){
 const c=byKey.get(selected),s=c.smart,pct=percent(c),w=warn(c);
 $('drawer-crumb').textContent=c.host+' / '+(c.role==='hdd'?'HDD '+c.index:c.label);
 $('drawer-title').textContent='/dev/'+c.device;
 $('drawer-model').innerHTML=esc(s.model)+'<br>serial …'+esc(s.serial_suffix)+' · '+n(s.temperature,0)+'°C';
 let detail='';
 if(c.role==='hdd'){
  detail=`<div class="detail-section"><div class="detail-title">Disk health<span>SMART · physical disk</span></div>${w?`<div class="detail-alert">${n(s.pending,0)} pending sectors · ${n(s.uncorrectable,0)} uncorrectable<br>Pending change / 24h: ${s.pending_delta>=0?'+':''}${n(s.pending_delta,0)}</div>`:''}${table(row('Overall assessment',s.health===1?'Passed':s.health===0?'Failed':'Unknown')+row('Reallocated sectors',n(s.reallocated,0))+row('Reallocated change / 24h',n(s.reallocated_delta,0))+row('Pending sectors',n(s.pending,0))+row('Offline uncorrectable',n(s.uncorrectable,0)))}</div><div class="detail-section"><div class="detail-title">Cell activity<span>5m average</span></div>${table(row('Read / append RPS',n(c.read_rps,2)+' / '+n(c.write_rps,2))+row('Read / append bandwidth',bytes(c.read_bps)+'/s / '+bytes(c.write_bps)+'/s')+row('Operation failures / 24h',n(c.errors,0))+row('Read cache payload',bytes(c.cache))+row('Flush queue',n(c.flush_queue,0)+' blocks')+row('Read / write queue',n(c.read_queue,0)+' / '+n(c.write_queue,0)))}</div>`;
 }else{
  detail=`<div class="detail-section"><div class="detail-title">Partition I/O<span>${n(c.io_sample_seconds,0)}s sample</span></div>${table(row('Device',esc('/dev/'+c.device))+row('Filesystem','XFS')+row('Mount',esc(c.mount))+row('Read + write operations',n(c.iops,2)+' IOPS')+row('Read / write bandwidth',bytes(c.read_bps)+'/s / '+bytes(c.write_bps)+'/s')+row('SSD SMART assessment',s.health===1?'Passed':s.health===0?'Failed':'Unknown'))}</div>`;
  if(c.role==='etcd')detail+=`<div class="detail-section"><div class="detail-title">${esc(c.etcd_job)}<span>S3 metadata</span></div>${table(row('Member role',state(c))+row('Leader known',c.has_leader===1?'Yes':'No')+row('Database size',bytes(c.db_size))+row('Database quota',bytes(c.quota))+row('WAL fsync p99 / 5m',n(scaled(c.fsync_p99,1000),2)+' ms')+row('Completed unary RPCs',n(c.activity,2)+'/s')+row('Failed Raft proposals / 24h',n(c.errors,0)))}</div>`;
  else detail+=`<div class="detail-section"><div class="detail-title">${c.role==='load'?'Read cache':'Write buffer'}<span>cell ${c.cell_id}</span></div><div class="detail-note">${c.role==='load'?'LOAD is an LRU cache. A full partition is normal: old cache entries are evicted when space is needed.':'STORE holds data waiting to be flushed to the HDD. The queue history shows complete 2 MiB blocks.'}<br>Per-partition error counters are not exported.</div></div>`;
 }
 const capacityNote=c.role==='hdd'?'Allocated S3 partition space, including redundancy.':c.role==='etcd'?'Filesystem usage includes database, WAL and filesystem overhead. The history below is database size.':'Filesystem usage from the volume collector. The history below is payload size, excluding filesystem overhead.';
 $('drawer-content').innerHTML=`<div class="drawer-status"><span style="color:var(--${w?'warn':'good'})"><i class="dot${w?' warn':''}"></i> ${state(c)}</span></div><div class="detail-title">${c.role==='hdd'?'S3 allocation':'Partition capacity'}<span>${n(pct)}% used</span></div><div class="detail-capacity">${bytes(c.used)} <span>/ ${bytes(c.capacity)}</span></div><div class="capacity-bar"><span style="width:${pct||0}%"></span></div><div class="detail-two"><div><label>FREE SPACE</label><strong>${bytes(c.free,2)}</strong></div><div><label>${c.role==='hdd'?'FILL':'PAYLOAD CHANGE'} / H</label><strong>${growth(c.growth)}</strong></div></div><div class="detail-note" style="margin-top:15px">${capacityNote}</div><div class="detail-section"><div class="detail-title">${c.capacity_label}<span>${hours}h · auto-scaled</span></div>${largeChart(c,'capacity')}</div><div class="detail-section"><div class="detail-title">${c.activity_label}<span>${hours}h · 5m average</span></div>${largeChart(c,'activity')}</div>${detail}<div class="drawer-foot">Snapshot: ${new Date(data.captured_at).toLocaleDateString('en-GB',{timeZone:'UTC'})}, ${time(data.timestamp)} UTC.<br>${c.role!=='hdd'?'Filesystem and I/O sampled at '+(c.captured_at?time(c.captured_at):'—')+' UTC.<br>':''}Updates every 30 seconds. GB and TB are decimal.</div>`;
 $$('[data-chart]').forEach(panel=>{panel.onpointermove=e=>{const values=points(c,panel.dataset.chart),rect=panel.getBoundingClientRect(),f=Math.max(0,Math.min(1,(e.clientX-rect.left)/rect.width)),target=data.timestamp-hours*3600+f*hours*3600;const p=values.reduce((a,b)=>Math.abs(b[0]-target)<Math.abs(a[0]-target)?b:a,values[0]);if(!p)return;const tip=panel.querySelector('.chart-tooltip');tip.hidden=false;tip.textContent=time(p[0])+' · '+(panel.dataset.chart==='capacity'?bytes(p[1],2):n(p[1],2)+' '+c.activity_label);};panel.onpointerleave=()=>panel.querySelector('.chart-tooltip').hidden=true;});
}
function openDrawer(key){selected=key;lastFocus=document.activeElement;renderDrawer();$('drawer').inert=false;document.body.classList.add('drawer-open');$('close-drawer').focus();}
function closeDrawer(){document.body.classList.remove('drawer-open');$('drawer').inert=true;selected=null;if(lastFocus?.isConnected)lastFocus.focus();}
$('close-drawer').onclick=closeDrawer;$('backdrop').onclick=closeDrawer;
document.addEventListener('keydown',e=>{if(!selected)return;if(e.key==='Escape')closeDrawer();if(e.key==='Tab'){e.preventDefault();$('close-drawer').focus();}});
$$('[data-filter]').forEach(b=>b.onclick=()=>{filter=b.dataset.filter;applyFilter();});
$$('[data-view]').forEach(b=>b.onclick=()=>{if(b.dataset.view==='buckets')location.href='/buckets';else document.querySelector('main').scrollTo({top:0});});
document.querySelector('.logo').onclick=e=>{e.preventDefault();document.querySelector('main').scrollTo({top:0});};
document.documentElement.dataset.theme=matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light';
$('theme').onclick=()=>{document.documentElement.dataset.theme=document.documentElement.dataset.theme==='dark'?'light':'dark';};
let pending=false,refreshTimer;
function connection(error){
 const stale=!!error||(data.timestamp&&Date.now()/1000-data.timestamp>100);
 document.body.classList.toggle('connection-stale',stale);
 $('connection').textContent=stale?(items.length?'Stale':'Unavailable'):'Live';
 $('connection').title=error||'';
 if(data.timestamp){$('rail-time').textContent=time(data.timestamp)+' UTC';$('rail-time').dateTime=data.captured_at;}
}
async function refresh(){
 if(pending||document.hidden)return;
 pending=true;clearTimeout(refreshTimer);
 const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),12000);
 try{
  const response=await fetch('/api/cluster',{cache:'no-store',signal:controller.signal});
  const next=await response.json();
  if(!response.ok)throw new Error(next.error||'Cluster metrics unavailable');
  if(!Array.isArray(next.cells)||!Array.isArray(next.partitions)||!Array.isArray(next.hosts))throw new Error('Invalid cluster metrics');
  const focused=document.activeElement?.dataset.key;
  data=next;indexData();renderSummary();render();
  $('summary').hidden=false;$('load-error').hidden=true;
  if(selected){if(byKey.has(selected))renderDrawer();else closeDrawer();}
  if(focused){const el=$$('[data-key]').find(el=>el.dataset.key===focused);if(el)el.focus({preventScroll:true});}
  connection(data.error);
 }catch(error){
  connection(error.message||'Cluster metrics unavailable');
  if(!items.length){$('load-error').hidden=false;$('load-error').textContent=error.message||'Cluster metrics unavailable';}
 }finally{clearTimeout(timeout);pending=false;refreshTimer=setTimeout(refresh,30000);}
}
document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
refresh();
})();
