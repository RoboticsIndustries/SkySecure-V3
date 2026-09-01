const API = window.SKYSECURE_API_URL || '';
const WS_URL = window.SKYSECURE_WS_URL || ((window.location.protocol === 'https:' ? 'wss:' : 'ws:') + '//' + window.location.host + '/ws/tracks');

const MIL_RANGES=[
  [0xADF000,0xADFFFF],[0xAE0000,0xAFFFFF],[0xA9F000,0xA9FFFF],
  [0x43C000,0x43CFFF],[0x43D000,0x43D7FF],
  [0x3C4000,0x3C47FF],[0x3C6000,0x3C63FF],[0x3C0000,0x3C07FF],
  [0x47A000,0x47AFFF],[0x340000,0x3407FF],
  [0x480000,0x4807FF],[0x4D2000,0x4D2FFF],[0x4D0000,0x4D0FFF],
];

const AIRPORTS=[
  {iata:'JFK',name:'New York JFK',      lat:40.6413,lon:-73.7781},
  {iata:'LAX',name:'Los Angeles',       lat:33.9425,lon:-118.408},
  {iata:'ORD',name:"Chicago O'Hare",    lat:41.9742,lon:-87.9073},
  {iata:'LHR',name:'London Heathrow',   lat:51.4775,lon:-0.4614 },
  {iata:'CDG',name:'Paris CDG',         lat:49.0097,lon:2.5479  },
  {iata:'AMS',name:'Amsterdam Schiphol',lat:52.3105,lon:4.7683  },
  {iata:'FRA',name:'Frankfurt',         lat:50.0379,lon:8.5622  },
  {iata:'DXB',name:'Dubai',             lat:25.2532,lon:55.3657 },
  {iata:'SIN',name:'Singapore Changi',  lat:1.3644, lon:103.992 },
  {iata:'NRT',name:'Tokyo Narita',      lat:35.7720,lon:140.393 },
  {iata:'SYD',name:'Sydney',            lat:-33.946,lon:151.177 },
  {iata:'GRU',name:'Sao Paulo',         lat:-23.436,lon:-46.473 },
  {iata:'ATL',name:'Atlanta',           lat:33.6407,lon:-84.428 },
  {iata:'DFW',name:'Dallas Fort Worth', lat:32.8998,lon:-97.040 },
  {iata:'MIA',name:'Miami',             lat:25.7959,lon:-80.287 },
  {iata:'SEA',name:'Seattle',           lat:47.4502,lon:-122.31 },
  {iata:'BOS',name:'Boston',            lat:42.3656,lon:-71.010 },
  {iata:'YYZ',name:'Toronto Pearson',   lat:43.6777,lon:-79.625 },
  {iata:'ICN',name:'Seoul Incheon',     lat:37.4602,lon:126.441 },
  {iata:'MEX',name:'Mexico City',       lat:19.4361,lon:-99.072 },
];

const EMERGENCY_SQUAWKS={'7500':'HIJACK','7600':'RADIO FAILURE','7700':'EMERGENCY'};

let allAC=[],markers={},aptLayers=[],map,coverageCircle,currentCoverage=null;
let historicalEvents=[],historicalLayerGroup=null,hotspotData=[],hotspotLayers=[],worldScanEnabled=false;
let conflictLayerGroup=null,_conflictAnalysis={conflicts:[]};
let hotspotWindowHours=24,hotspotRecencyHours=2;
const HISTORICAL_COLOR='#a855f7';
let ch1=null,ch2=null;
const acHistory={},acEvents={};
const MAX_PTS=120;
let openIcao=null,hpSpd=null,hpAlt=null,hpHdg=null;
let aiRC=null,aiSC=null;
let csvRows=[],csvHeaders=[];
let watchEvents=[],watchSummary=null,watchKindFilter='ALL',watchGhostGroup=null;

if(window.Chart){
  Chart.defaults.color='#8b9bb4';
  Chart.defaults.borderColor='rgba(56,189,248,0.08)';
  Chart.defaults.font.family="'JetBrains Mono',ui-monospace,Menlo,Consolas,monospace";
}

function esc(value){
  return String(value??'').replace(/[&<>'"]/g,ch=>({
    '&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'
  })[ch]);
}

// ── Scoring ───────────────────────────────────────────────────
function spoofScore(ac){
  let s=0;
  if(ac.lat!=null&&(ac.lat<-90||ac.lat>90))  s+=70;
  if(ac.lon!=null&&(ac.lon<-180||ac.lon>180)) s+=70;
  if(ac.alt===999999) s+=65;
  if(ac.vel===9999)   s+=65;
  if(ac.vel!=null&&ac.vel>2000)  s+=55;
  else if(ac.vel!=null&&ac.vel>1400) s+=25;
  if(ac.alt!=null&&ac.alt>100000) s+=50;
  if(ac.anoms){
    if(ac.anoms.includes('GNSS_SPOOF'))     s+=55;
    if(ac.anoms.includes('IDENTITY_SPOOF')) s+=60;
    if(ac.anoms.includes('DUPLICATE_ICAO')) s+=65;
    if(ac.anoms.includes('TELEPORTATION'))  s+=45;
  }
  return Math.min(100,Math.round(s));
}

function checkMil(ac){
  if(!ac.icao) return false;
  const v=parseInt(ac.icao,16);
  if(isNaN(v)) return false;
  return MIL_RANGES.some(([a,b])=>v>=a&&v<=b);
}

function mkColor(ac){
  if(ac.sqk&&EMERGENCY_SQUAWKS[ac.sqk]) return '#ffb020';
  const sp=spoofScore(ac);
  if(sp>=60)                   return '#ff2d55';
  if(sp>=30)                   return '#f97316';
  if(checkMil(ac))             return '#ff2d55';
  if(ac.cls==='DARK_AIRCRAFT') return '#a855f7';
  return '#2fe6a7';
}

// ── History ───────────────────────────────────────────────────
function recordHistory(a){
  if(!a.icao) return;
  if(!acHistory[a.icao]) acHistory[a.icao]=[];
  if(!acEvents[a.icao])  acEvents[a.icao]=[];
  const now=new Date(),ts=now.toISOString().substr(11,8)+' UTC';
  const prev=acHistory[a.icao].at(-1);
  const pt={t:now,ts,vel:a.vel??null,alt:a.alt??null,hdg:a.hdg??null,sp:spoofScore(a)};
  if(prev){
    const dt=(now-prev.t)/1000;
    if(prev.vel!=null&&pt.vel!=null){
      const dv=Math.abs(pt.vel-prev.vel);
      if(dv>200&&dt<60) pushEvent(a.icao,ts,'red',
        'Speed jumped from '+Math.round(prev.vel)+' kts to '+Math.round(pt.vel)+' kts (+'+Math.round(dv)+' kts in '+Math.round(dt)+'s)');
    }
    if(prev.alt!=null&&pt.alt!=null){
      const da=Math.abs(pt.alt-prev.alt);
      if(da>5000&&da>dt*200) pushEvent(a.icao,ts,'red',
        'Altitude jumped from '+prev.alt.toLocaleString()+' ft to '+pt.alt.toLocaleString()+' ft in '+Math.round(dt)+'s — physically impossible');
    }
    if(prev.sp<30&&pt.sp>=30) pushEvent(a.icao,ts,'red','Spoof probability crossed 30% — now '+pt.sp+'%');
  }
  acHistory[a.icao].push(pt);
  if(acHistory[a.icao].length>MAX_PTS) acHistory[a.icao].shift();
}

function pushEvent(icao,ts,sev,msg){
  acEvents[icao].unshift({ts,sev,msg});
  if(acEvents[icao].length>50) acEvents[icao].pop();
  if(openIcao===icao) renderPanel(icao);
}

// ── History Panel ─────────────────────────────────────────────
function openPanel(icao){
  openIcao=icao;
  document.getElementById('hp').classList.add('open');
  renderPanel(icao);
}
function openAircraftDetails(icao){
  const normalized=String(icao||'').trim().toUpperCase();
  if(!normalized) return;
  const live=allAC.find(a=>a.icao===normalized);
  const retained=historicalEvents.filter(event=>event.icao24===normalized);
  if(!live&&!retained.length) return;
  if(live&&live.lat!=null&&live.lon!=null) map.flyTo([live.lat,live.lon],9,{duration:0.8});
  else if(retained[0]?.lat!=null&&retained[0]?.lon!=null) map.flyTo([retained[0].lat,retained[0].lon],9,{duration:0.8});
  openPanel(normalized);
}
function closePanel(){
  document.getElementById('hp').classList.remove('open');
  openIcao=null;
}
function renderPanel(icao){
  const retained=historicalEvents.filter(event=>event.icao24===icao);
  const latest=retained[0];
  const ac=allAC.find(a=>a.icao===icao)||(latest?{
    icao,cs:latest.callsign,lat:latest.lat,lon:latest.lon,
    alt:null,vel:null,hdg:null,risk:latest.risk_score||0,
    anoms:retained.map(event=>event.anomaly_type),cls:'HISTORICAL'
  }:null);
  const hist=acHistory[icao]||[];
  const evts=[...(acEvents[icao]||[]),...retained.map(event=>({
    ts:event.time,sev:(event.risk_score||0)>=76?'red':'orange',
    msg:(event.detector||event.anomaly_type||'Anomaly')+': '+(event.description||'retained evidence')
  }))];
  if(!ac) return;
  const sp=spoofScore(ac),mil=checkMil(ac);
  const cls=mil?'CONFIRMED MILITARY':(ac.cls||'UNKNOWN').replace(/_/g,' ');
  document.getElementById('hp-title').textContent=icao+(ac.cs?' - '+ac.cs:'');
  document.getElementById('hp-sub').textContent=
    (ac.alt?ac.alt.toLocaleString()+' ft':'--')+'  |  '+
    (ac.vel?Math.round(ac.vel)+' kts':'--')+'  |  '+
    (ac.hdg?Math.round(ac.hdg)+'deg':'--')+'  |  '+hist.length+' data points';
  document.getElementById('hp-badges').innerHTML=
    '<span class="badge '+(mil?'mil':sp>=30?'spoof':'civil')+'">'+esc(cls)+'</span> '+
    '<span class="badge '+(sp>=60?'spoof':sp>=30?'sus':'clean')+'">Spoof '+sp+'%</span> '+
    '<span class="badge gray">Risk '+(ac.risk||0)+'/100</span>'+
    (ac.sqk?'<span class="badge sus">SQK '+esc(ac.sqk)+'</span>':'')+
    (evts.length?'<span class="badge spoof">'+evts.length+' events</span>':'');
  const labels=hist.map(p=>p.ts.substr(0,8));
  const vels=hist.map(p=>p.vel),alts=hist.map(p=>p.alt),hdgs=hist.map(p=>p.hdg);
  const sA=[],aA=[];
  hist.forEach((p,i)=>{
    if(!i) return;
    const pv=hist[i-1];
    if(pv.vel!=null&&p.vel!=null&&Math.abs(p.vel-pv.vel)>200) sA.push({x:labels[i],y:p.vel});
    if(pv.alt!=null&&p.alt!=null&&Math.abs(p.alt-pv.alt)>5000) aA.push({x:labels[i],y:p.alt});
  });
  const opts=(unit)=>({responsive:true,maintainAspectRatio:false,animation:false,
    plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>(c.dataset.label==='Anomaly'?'ANOMALY: ':'')+
      (c.parsed.y!=null?Math.round(c.parsed.y)+' '+unit:'--')}}},
    scales:{x:{ticks:{font:{size:8},maxRotation:0,maxTicksLimit:6},grid:{color:'rgba(56,189,248,0.07)'}},
            y:{ticks:{font:{size:9}},grid:{color:'rgba(56,189,248,0.07)'}}}});
  if(hpSpd) hpSpd.destroy();
  hpSpd=new Chart(document.getElementById('hp-spd').getContext('2d'),{data:{labels,datasets:[
    {type:'line',label:'Speed',data:vels,borderColor:'#22d3ee',backgroundColor:'rgba(34,211,238,0.08)',borderWidth:2,pointRadius:0,fill:true,tension:0.3,spanGaps:true},
    {type:'scatter',label:'Anomaly',data:sA,backgroundColor:'#ff2d55',pointRadius:7,pointStyle:'triangle'}
  ]},options:opts('kts')});
  if(hpAlt) hpAlt.destroy();
  hpAlt=new Chart(document.getElementById('hp-alt').getContext('2d'),{data:{labels,datasets:[
    {type:'line',label:'Altitude',data:alts,borderColor:'#a855f7',backgroundColor:'rgba(168,85,247,0.08)',borderWidth:2,pointRadius:0,fill:true,tension:0.3,spanGaps:true},
    {type:'scatter',label:'Anomaly',data:aA,backgroundColor:'#ff2d55',pointRadius:7,pointStyle:'triangle'}
  ]},options:opts('ft')});
  if(hpHdg) hpHdg.destroy();
  hpHdg=new Chart(document.getElementById('hp-hdg').getContext('2d'),{type:'line',data:{labels,datasets:[
    {label:'Heading',data:hdgs,borderColor:'#ffb020',backgroundColor:'rgba(255,176,32,0.08)',borderWidth:2,pointRadius:0,fill:true,tension:0.3,spanGaps:true}
  ]},options:opts('deg')});
  document.getElementById('hp-evts').innerHTML=evts.length?evts.map(e=>
    '<div class="evt '+e.sev+'"><div class="evtdot" style="background:'+(e.sev==='red'?'#ff2d55':'#f97316')+'"></div>'+
    '<div><div class="evttime">'+esc(e.ts)+'</div><div class="evtmsg">'+esc(e.msg)+'</div></div></div>'
  ).join(''):'<div class="dim">No anomalies detected yet</div>';
}

// ── Map ───────────────────────────────────────────────────────
function initMap(){
  map=L.map('map',{preferCanvas:true}).setView([30,10],3);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{
    attribution:'OpenStreetMap / CARTO',subdomains:'abcd',maxZoom:19
  }).addTo(map);
  historicalLayerGroup=L.layerGroup().addTo(map);
  conflictLayerGroup=L.layerGroup().addTo(map);
  watchGhostGroup=L.layerGroup().addTo(map);
  const sel=document.getElementById('apt-sel');
  AIRPORTS.forEach(a=>{
    const o=document.createElement('option');
    o.value=a.lat+','+a.lon; o.textContent=a.iata+' — '+a.name;
    sel.appendChild(o);
  });
  const coverageSelect=document.getElementById('coverage-airport');
  AIRPORTS.forEach(a=>{
    const o=document.createElement('option');
    o.value=a.iata; o.textContent=a.iata+' — '+a.name;
    coverageSelect.appendChild(o);
  });
  drawAirports();
}

function drawAirports(){
  aptLayers.forEach(l=>map.removeLayer(l)); aptLayers=[];
  if(!document.getElementById('chk-apt').checked) return;
  AIRPORTS.forEach(a=>{
    const c=L.circle([a.lat,a.lon],{radius:27780,color:'#22d3ee',fillColor:'#22d3ee',fillOpacity:0.03,weight:1,dashArray:'4 4',opacity:0.3}).addTo(map);
    const lbl=L.marker([a.lat,a.lon],{icon:L.divIcon({className:'',
      html:'<div style="color:#67e8f9;font-size:10px;font-weight:600;white-space:nowrap;text-shadow:0 1px 3px #000">'+a.iata+'</div>',
      iconAnchor:[10,0]})}).addTo(map);
    aptLayers.push(c,lbl);
  });
}

function flyTo(){
  const v=document.getElementById('apt-sel').value; if(!v) return;
  const[lat,lon]=v.split(',').map(Number);
  map.flyTo([lat,lon],9,{duration:1.2});
  setTimeout(()=>document.getElementById('apt-sel').value='',100);
}

function drawCoverageArea(area){
  if(coverageCircle) map.removeLayer(coverageCircle);
  coverageCircle=L.circle([area.latitude,area.longitude],{
    radius:area.radius_nm*1852,color:'#22d3ee',fillColor:'#22d3ee',
    fillOpacity:0.03,weight:1.5,dashArray:'7 5',interactive:false
  }).addTo(map);
}

function fillCoverage(area){
  currentCoverage=area;
  previewCoverageArea(area);
}

function previewCoverageArea(area){
  document.getElementById('coverage-lat').value=Number(area.latitude).toFixed(4);
  document.getElementById('coverage-lon').value=Number(area.longitude).toFixed(4);
  document.getElementById('coverage-radius').value=area.radius_nm;
  drawCoverageArea(area);
}

function insideCoverage(ac){
  if(!currentCoverage||ac.lat==null||ac.lon==null) return true;
  const rad=Math.PI/180;
  const lat1=currentCoverage.latitude*rad,lat2=ac.lat*rad;
  const dlat=lat2-lat1,dlon=(ac.lon-currentCoverage.longitude)*rad;
  const h=Math.sin(dlat/2)**2+Math.cos(lat1)*Math.cos(lat2)*Math.sin(dlon/2)**2;
  const distanceNm=3440.065*2*Math.asin(Math.min(1,Math.sqrt(h)));
  return distanceNm<=currentCoverage.radius_nm;
}

async function loadCoverage(){
  const status=document.getElementById('coverage-status');
  try{
    const r=await fetch(API+'/api/coverage');
    if(!r.ok) throw new Error('HTTP '+r.status);
    const area=(await r.json()).coverage;
    fillCoverage(area);
    status.textContent='Active: '+area.label+' · '+area.radius_nm+' NM';
  }catch(e){ status.textContent='Coverage settings unavailable'; }
}

function getOperatorKey(forcePrompt=false){
  let key=forcePrompt?'':sessionStorage.getItem('skysecureOperatorKey');
  if(!key) key=window.prompt('SkySecure operator key');
  if(!key) throw new Error('operator authorization required');
  sessionStorage.setItem('skysecureOperatorKey',key);
  return key;
}

async function operatorFetch(path,options={},authorizationError='Operator authorization failed'){
  const send=key=>fetch(API+path,{...options,headers:{
    ...(options.headers||{}),'X-SkySecure-Operator-Key':key
  }});
  let key=getOperatorKey();
  let r=await send(key);
  if(r.status===403){
    sessionStorage.removeItem('skysecureOperatorKey');
    try{ key=getOperatorKey(true); }
    catch(_error){ throw new Error(authorizationError); }
    r=await send(key);
  }
  if(r.status===403){
    sessionStorage.removeItem('skysecureOperatorKey');
    throw new Error(authorizationError);
  }
  if(r.status===503){
    await new Promise(resolve=>setTimeout(resolve,300));
    r=await send(key);
  }
  return r;
}

function selectCoverageAirport(){
  const iata=document.getElementById('coverage-airport').value;
  const airport=AIRPORTS.find(a=>a.iata===iata); if(!airport) return;
  previewCoverageArea({latitude:airport.lat,longitude:airport.lon,radius_nm:Number(document.getElementById('coverage-radius').value)||250});
  map.flyTo([airport.lat,airport.lon],6,{duration:1.0});
}

function useMapCenterForCoverage(){
  const center=map.getCenter();
  document.getElementById('coverage-airport').value='';
  document.getElementById('coverage-lat').value=center.lat.toFixed(4);
  document.getElementById('coverage-lon').value=center.lng.toFixed(4);
}

async function applyCoverage(){
  const lat=Number(document.getElementById('coverage-lat').value);
  const lon=Number(document.getElementById('coverage-lon').value);
  const radius=Number(document.getElementById('coverage-radius').value);
  const iata=document.getElementById('coverage-airport').value;
  const airport=AIRPORTS.find(a=>a.iata===iata);
  const status=document.getElementById('coverage-status');
  if(!Number.isFinite(lat)||lat < -90||lat > 90||!Number.isFinite(lon)||lon < -180||lon > 180||!Number.isInteger(radius)||radius < 1||radius > 250){
    status.textContent='Enter valid coordinates and a 1–250 NM radius'; return;
  }
  const area={latitude:lat,longitude:lon,radius_nm:radius,label:airport?(airport.iata+' — '+airport.name):'Custom map area'};
  status.textContent='Switching live feed...';
  try{
    const r=await operatorFetch('/api/coverage',{method:'PUT',headers:{
      'Content-Type':'application/json'
    },body:JSON.stringify(area)});
    if(!r.ok) throw new Error('HTTP '+r.status);
    fillCoverage(area); map.flyTo([lat,lon],6,{duration:1.0});
    _directCache={}; _backendCache={}; mergeAndRender();
    status.textContent='Active: '+area.label+' · '+radius+' NM';
    await pollLive();
  }catch(e){
    if(currentCoverage) previewCoverageArea(currentCoverage);
    document.getElementById('coverage-airport').value='';
    status.textContent='Coverage update failed: '+e.message;
  }
}

async function loadWorldScan(){
  const status=document.getElementById('world-scan-status');
  try{
    const r=await fetch(API+'/api/world-scan');
    if(!r.ok) throw new Error('HTTP '+r.status);
    const data=await r.json(),scan=data.scan||{};
    worldScanEnabled=Boolean(scan.enabled);
    document.getElementById('world-scan-dwell').value=String(scan.dwell_seconds||360);
    document.getElementById('world-scan-toggle').textContent=worldScanEnabled?'Stop world scan':'Start world scan';
    status.textContent=worldScanEnabled?
      'Active: '+data.current_tile.label+' · tile '+(scan.tile_index+1)+'/'+data.tile_count:
      'Stopped';
  }catch(e){ status.textContent='Scanner unavailable'; }
}

async function updateWorldScan(){
  const status=document.getElementById('world-scan-status');
  const button=document.getElementById('world-scan-toggle');
  const dwell=Number(document.getElementById('world-scan-dwell').value);
  button.disabled=true;
  try{
    status.textContent=worldScanEnabled?'Stopping scanner...':'Starting worldwide rotation...';
    const r=await operatorFetch('/api/world-scan',{method:'PUT',headers:{
      'Content-Type':'application/json'
    },body:JSON.stringify({enabled:!worldScanEnabled,dwell_seconds:dwell})},'Scanner authorization failed');
    if(!r.ok) throw new Error('HTTP '+r.status);
    await loadWorldScan(); await loadCoverage(); await pollLive();
  }catch(e){ status.textContent='Scanner update failed: '+e.message; }
  finally{ button.disabled=false; }
}

function clearMapLayers(layers){
  layers.forEach(layer=>map.removeLayer(layer));
  layers.length=0;
}

function renderHistoricalAnomalies(events=historicalEvents){
  historicalEvents=events||historicalEvents;
  if(!historicalLayerGroup) return;
  historicalLayerGroup.clearLayers();
  if(!document.getElementById('history-markers').checked) return;
  historicalEvents.forEach(event=>{
    if(event.lat==null||event.lon==null) return;
    const marker=L.circleMarker([event.lat,event.lon],{
      radius:Math.max(7,Math.min(12,Math.round((event.risk_score||40)/12))),
      color:HISTORICAL_COLOR,fillColor:HISTORICAL_COLOR,fillOpacity:0.75,weight:1
    }).addTo(historicalLayerGroup);
    marker.bindPopup('<div class="pt">Historical anomaly · '+esc(event.icao24||'unknown')+'</div>'+
      '<div class="pr"><span class="pk">Observed</span><b>'+esc(event.time)+'</b></div>'+
      '<div class="pr"><span class="pk">Layer / detector</span><b>'+esc(event.layer||'--')+' / '+esc(event.detector||'--')+'</b></div>'+
      '<div class="pr"><span class="pk">Evidence</span><span>'+esc(event.description||event.anomaly_type||'--')+'</span></div>'+
      '<button class="view-hist" data-aircraft-details="'+esc(event.icao24||'')+'" data-close-popup="1">Aircraft details</button>'+
      '<div style="color:'+HISTORICAL_COLOR+';font-size:10px;margin-top:6px">Historical public-feed evidence; not independent spoof confirmation.</div>');
  });
}

function renderHotspots(hotspots=hotspotData){
  hotspotData=hotspots||hotspotData;
  clearMapLayers(hotspotLayers);
  if(!document.getElementById('hotspot-layer').checked) return;
  hotspotData.forEach(hotspot=>{
    const count=Number(hotspot.event_count)||1;
    const confidence=hotspot.confidence||'emerging';
    const aircraftIds=Array.isArray(hotspot.aircraft_ids)?hotspot.aircraft_ids:[];
    const aircraftLinks=aircraftIds.slice(0,20).map(icao=>
      '<button class="view-hist" data-aircraft-details="'+esc(icao)+'" data-close-popup="1">'+esc(icao)+' details</button>'
    ).join('')+(aircraftIds.length>20?'<div style="font-size:10px;margin-top:4px">+'+esc(aircraftIds.length-20)+' more aircraft</div>':'');
    const color={critical:'#ff2d55',confirmed:'#f97316',emerging:'#ffb020'}[confidence]||'#ffb020';
    const layer=L.circle([hotspot.lat,hotspot.lon],{
      radius:Math.min(180000,25000+Math.sqrt(count)*22000),
      color,fillColor:color,fillOpacity:Math.min(0.42,0.10+count/80),weight:1
    }).addTo(map);
    layer.bindPopup('<div class="pt">Anomaly hotspot</div>'+
      '<div class="pr"><span class="pk">Confidence</span><b>'+esc(confidence.toUpperCase())+'</b></div>'+
      '<div class="pr"><span class="pk">Evidence window</span><b>'+esc(hotspotWindowHours)+' hours</b></div>'+
      '<div class="pr"><span class="pk">Active within</span><b>'+esc(hotspotRecencyHours)+' hours</b></div>'+
      '<div class="pr"><span class="pk">Deduplicated events</span><b>'+count+'</b></div>'+
      '<div class="pr"><span class="pk">Raw observations</span><b>'+esc(hotspot.raw_event_count||count)+'</b></div>'+
      '<div class="pr"><span class="pk">Aircraft</span><b>'+esc(hotspot.aircraft_count||0)+'</b></div>'+
      '<div class="pr"><span class="pk">Peak risk</span><b>'+esc(hotspot.max_risk||0)+'/100</b></div>'+
      '<div style="font-size:10px;font-weight:700;margin-top:8px">Contributing aircraft</div>'+aircraftLinks);
    hotspotLayers.push(layer);
  });
}

async function loadHistoricalAnomalies(){
  const hours=Number(document.getElementById('history-window').value);
  try{
    const history=await fetch(API+'/api/anomalies/history?hours='+hours+'&limit=10000');
    if(!history.ok) throw new Error('history HTTP '+history.status);
    historicalEvents=(await history.json()).events||[];
    renderHistoricalAnomalies(historicalEvents);
    const hotspots=await fetch(API+'/api/anomalies/hotspots?hours='+hours+'&precision=1');
    if(!hotspots.ok) throw new Error('hotspots HTTP '+hotspots.status);
    const hotspotResponse=await hotspots.json();
    hotspotData=hotspotResponse.hotspots||[];
    hotspotWindowHours=hotspotResponse.hotspot_hours||24;
    hotspotRecencyHours=hotspotResponse.recency_hours||2;
    renderHotspots(hotspotData);
  }catch(e){ console.warn('[anomaly-history] load failed:',e.message); }
}

function applyFilters(){ renderAircraft(allAC); }

function renderAircraft(ac){
  allAC=ac;
  const spoofOnly=document.getElementById('chk-spoof').checked;
  const milOnly  =document.getElementById('chk-mil').checked;
  const seen=new Set();
  let civil=0,mil=0,dark=0,spf=0;
  ac.forEach(a=>{
    if(a.lat==null||a.lon==null||isNaN(a.lat)||isNaN(a.lon)) return;
    if(a.lat<-90||a.lat>90||a.lon<-180||a.lon>180) return;
    seen.add(a.icao);
    recordHistory(a);
    const sp=spoofScore(a),isMil=checkMil(a),color=mkColor(a);
    if(isMil) mil++; else if(a.cls==='DARK_AIRCRAFT') dark++; else civil++;
    if(sp>=30) spf++;
    const vis=(!spoofOnly||sp>=30)&&(!milOnly||isMil);
    if(markers[a.icao]){
      markers[a.icao].setLatLng([a.lat,a.lon]);
      markers[a.icao].setStyle({color,fillColor:color,radius:sp>=50?5:3});
      vis?(!map.hasLayer(markers[a.icao])&&markers[a.icao].addTo(map)):map.removeLayer(markers[a.icao]);
    } else {
      const m=L.circleMarker([a.lat,a.lon],{radius:sp>=50?5:3,color,fillColor:color,fillOpacity:0.9,weight:0});
      m.bindPopup(()=>buildPopup(a),{maxWidth:260});
      if(vis) m.addTo(map);
      markers[a.icao]=m;
    }
  });
  Object.keys(markers).forEach(id=>{
    if(!seen.has(id)){map.removeLayer(markers[id]);delete markers[id];}
  });
  document.getElementById('ct').textContent=ac.length;
  document.getElementById('cc').textContent=civil;
  document.getElementById('cm').textContent=mil;
  document.getElementById('cd').textContent=dark;
  document.getElementById('cs').textContent=spf;
}

function buildPopup(a){
  const sp=spoofScore(a),col=sp>=60?'#ff2d55':sp>=30?'#f97316':'#2fe6a7';
  const cls=checkMil(a)?'CONFIRMED MILITARY':(a.cls||'UNKNOWN').replace(/_/g,' ');
  const evts=acEvents[a.icao]||[];
  return '<div class="pt">'+esc(a.icao)+(a.cs?' - '+esc(a.cs):'')+'</div>'+
    '<div class="pr"><span class="pk">Classification</span><b>'+esc(cls)+'</b></div>'+
    '<div class="pr"><span class="pk">Spoof probability</span><b style="color:'+col+'">'+sp+'%</b></div>'+
    (a.sqk?'<div class="pr"><span class="pk">Squawk</span><b style="color:#ffb020">'+esc(a.sqk)+(EMERGENCY_SQUAWKS[a.sqk]?' — '+EMERGENCY_SQUAWKS[a.sqk]:'')+'</b></div>':'')+
    '<div class="pr"><span class="pk">Altitude</span><span>'+(a.alt?a.alt.toLocaleString()+' ft':'--')+'</span></div>'+
    '<div class="pr"><span class="pk">Speed</span><span>'+(a.vel?Math.round(a.vel)+' kts':'--')+'</span></div>'+
    '<div class="pr"><span class="pk">Heading</span><span>'+(a.hdg?Math.round(a.hdg)+'deg':'--')+'</span></div>'+
    '<div class="pr"><span class="pk">Risk score</span><span>'+esc(a.risk||0)+'/100</span></div>'+
    (evts.length?'<div class="pr"><span class="pk" style="color:#ff2d55">Events</span><span style="color:#ff2d55;font-weight:700">'+evts.length+'</span></div>':'')+
    '<button class="view-hist" data-open-panel="'+esc(a.icao)+'" data-close-popup="1">View History and Charts</button>';
}

// ── Watch: transponder shutoff + military activity ────────────
const WATCH_KIND_LABEL={
  TRANSPONDER_OFF:'TRANSPONDER OFF',
  EMERGENCY_SQUAWK:'EMERGENCY SQUAWK',
  MIL_CONCENTRATION:'MIL CONCENTRATION',
  MIL_HIGH_PERFORMANCE:'HIGH PERFORMANCE',
};

function watchTime(value){
  const d=new Date(value);
  if(isNaN(d)) return String(value||'--');
  return d.toISOString().substr(11,8)+'Z';
}

async function loadWatchSummary(){
  const status=document.getElementById('watch-status');
  try{
    const r=await fetch(API+'/api/watch/summary');
    if(!r.ok) throw new Error('HTTP '+r.status);
    watchSummary=await r.json();
    if(watchSummary.analysis_available===false){
      status.textContent='Watch monitor starting — no snapshot yet';
      status.className='watch-status';
      return;
    }
    status.textContent='Live — scanned '+new Date(watchSummary.generated_at*1000).toISOString().substr(11,8)+' UTC';
    status.className='watch-status ok';
    document.getElementById('w-tracks').textContent=watchSummary.tracks_observed??'--';
    document.getElementById('w-fresh').textContent=watchSummary.tracks_fresh??'--';
    document.getElementById('w-ledger').textContent=watchSummary.ledger_size??'--';
    const concentrations=Array.isArray(watchSummary.active_concentrations)?watchSummary.active_concentrations:[];
    document.getElementById('w-conc').textContent=concentrations.length;
    document.getElementById('w-persist').textContent=watchSummary.history_persistence?'PostgreSQL':'Redis only';
    const wrap=document.getElementById('watch-conc-wrap');
    if(concentrations.length){
      wrap.style.display='block';
      document.getElementById('watch-conc-list').innerHTML=concentrations.map(c=>
        '<div class="conc-card"><b>'+esc(c.peak||0)+' aircraft</b> near '+esc(c.cell)+
        ' · since '+watchTime((c.started||0)*1000)+' · peak '+esc(c.peak||0)+
        '<br><span class="dim">'+esc((c.members||[]).join(', '))+'</span></div>'
      ).join('');
    } else {
      wrap.style.display='none';
    }
  }catch(e){
    watchSummary=null;
    status.textContent='Watch monitor unreachable';
    status.className='watch-status bad';
  }
}

async function loadWatchEvents(){
  try{
    const hours=Number(document.getElementById('watch-hours').value);
    const kind=watchKindFilter==='ALL'?'':'&kind='+encodeURIComponent(watchKindFilter);
    const r=await fetch(API+'/api/watch/events?hours='+hours+'&limit=300'+kind);
    if(!r.ok) throw new Error('HTTP '+r.status);
    watchEvents=(await r.json()).events||[];
    renderWatchFeed();
    renderWatchGhosts();
  }catch(e){
    console.warn('[watch] events load failed:',e.message);
  }
}

function renderWatchFeed(){
  const feed=document.getElementById('watch-feed');
  const pip=document.getElementById('watch-pip');
  if(pip) pip.textContent=watchEvents.length>99?'99+':String(watchEvents.length);
  if(!watchEvents.length){
    feed.innerHTML='<div class="dim watch-empty">No watch events in this window. The monitor emits only with live-coverage proof.</div>';
    return;
  }
  feed.innerHTML=watchEvents.map(event=>{
    const kind=event.kind||'';
    return '<div class="wevent k-'+esc(kind)+'" data-watch-lat="'+esc(event.lat??'')+'" data-watch-lon="'+esc(event.lon??'')+'">'+
      '<div class="wt">'+watchTime(event.time)+'</div>'+
      '<div class="wk">'+esc(WATCH_KIND_LABEL[kind]||kind)+'</div>'+
      '<div class="ws">'+esc(event.summary||'')+'</div>'+
      '<div class="wsev">SEV<b>'+esc(event.severity??'-')+'</b></div>'+
    '</div>';
  }).join('');
}

function renderWatchGhosts(){
  if(!watchGhostGroup) return;
  watchGhostGroup.clearLayers();
  if(!document.getElementById('watch-ghosts').checked) return;
  watchEvents.forEach(event=>{
    if(event.lat==null||event.lon==null) return;
    if(event.kind==='TRANSPONDER_OFF'){
      const peers=event.meta?.live_peers_within_nm?.count;
      L.circle([event.lat,event.lon],{
        radius:30000,color:'#ff2d55',weight:1.5,dashArray:'5 5',
        fillColor:'#ff2d55',fillOpacity:0.08
      }).addTo(watchGhostGroup);
      const marker=L.marker([event.lat,event.lon],{icon:L.divIcon({className:'',
        html:'<div style="color:#ff2d55;font-size:15px;font-weight:700;text-shadow:0 0 9px #ff2d55">✕</div>',
        iconAnchor:[7,8]})}).addTo(watchGhostGroup);
      marker.bindPopup('<div class="pt">Last known — '+esc(event.callsign||event.icao24||'unknown')+'</div>'+
        '<div class="pr"><span class="pk">Vanished</span><b>'+watchTime(event.time)+'</b></div>'+
        '<div class="pr"><span class="pk">Last altitude</span><span>'+esc(event.meta?.last_alt_ft??'--')+' ft</span></div>'+
        '<div class="pr"><span class="pk">Live peers nearby</span><span>'+esc(peers??'--')+'</span></div>'+
        '<div style="color:#ff2d55;font-size:10px;margin-top:6px">Suspected transponder shutoff under proven-live coverage.</div>');
    } else if(event.kind==='MIL_CONCENTRATION'){
      L.circle([event.lat,event.lon],{
        radius:60000,color:'#f97316',weight:1,dashArray:'3 4',
        fillColor:'#f97316',fillOpacity:0.05
      }).addTo(watchGhostGroup);
    }
  });
}

// ── Detection layer telemetry ─────────────────────────────────
async function updateLayerSummary(){
  try {
    const r=await fetch(API+'/api/layers');
    if(!r.ok) throw new Error('HTTP '+r.status);
    const data=await r.json();
    Object.entries(data.layers||{}).forEach(([name,layer])=>{
      const el=document.getElementById('layer-'+name);
      if(!el) return;
      el.textContent=(layer.triggered||0)+' / '+(layer.evaluated||0)+' / '+(layer.skipped||0);
      const reasons=Object.entries(layer.skipped_reasons||{}).map(([reason,count])=>count+'× '+reason);
      el.title=(layer.trigger_count||0)+' total triggers'+(reasons.length?'; skipped: '+reasons.join('; '):'');
      el.style.color=layer.triggered?'#ff2d55':layer.evaluated?'#2fe6a7':'#5c6b84';
    });
  } catch(e) {
    ['L1','L2','L3','L4','L5'].forEach(name=>{
      const el=document.getElementById('layer-'+name); if(el){el.textContent='offline';el.style.color='#5c6b84';}
    });
  }
}

async function updateLayerTriggers(){
  const layer=document.getElementById('layer-select').value;
  const body=document.getElementById('layer-trigger-body');
  try {
    const r=await fetch(API+'/api/layers/'+layer+'/triggers?limit=50');
    if(!r.ok) throw new Error('HTTP '+r.status);
    const data=await r.json();
    if(!data.triggers.length){
      body.innerHTML='<tr><td colspan="5" class="td-dim">No '+esc(layer)+' triggers in current tracks</td></tr>';
      return;
    }
    body.innerHTML=data.triggers.map(t=>'<tr>'+
      '<td><button class="view-hist" data-aircraft-details="'+esc(t.aircraft_id)+'">'+esc(t.aircraft_id)+'</button></td>'+
      '<td>'+esc(t.detector)+'</td><td>'+esc(t.type.replace(/_/g,' '))+'</td>'+
      '<td>+'+esc(t.score_delta)+'</td><td style="font-family:var(--mono);font-size:10px">'+esc(JSON.stringify(t.evidence))+'</td></tr>').join('');
  } catch(e) {
    body.innerHTML='<tr><td colspan="5" class="td-dim" style="color:#ff2d55">Layer telemetry unavailable</td></tr>';
  }
}

// ── EDA ───────────────────────────────────────────────────────
function updateEDA(ac){
  const tot=ac.length,spf=ac.filter(a=>spoofScore(a)>=30).length;
  const mil=ac.filter(a=>checkMil(a)||a.cls==='DARK_AIRCRAFT').length;
  document.getElementById('e-tot').textContent=tot;
  document.getElementById('e-sp').textContent=spf;
  document.getElementById('e-mil').textContent=mil;
  document.getElementById('e-cl').textContent=Math.max(0,tot-spf-mil);
  const cc={CIVILIAN:0,LIKELY_MILITARY:0,CONFIRMED_MILITARY:0,DARK_AIRCRAFT:0,UNKNOWN:0};
  ac.forEach(a=>{const k=checkMil(a)?'CONFIRMED_MILITARY':(a.cls||'UNKNOWN');if(k in cc)cc[k]++;else cc.UNKNOWN++;});
  if(ch1) ch1.destroy();
  ch1=new Chart(document.getElementById('ch-cls').getContext('2d'),{
    type:'doughnut',data:{labels:['Civilian','Likely Military','Confirmed Military','Dark','Unknown'],
    datasets:[{data:Object.values(cc),backgroundColor:['#2fe6a7','#f97316','#ff2d55','#a855f7','#5c6b84'],borderWidth:0}]},
    options:{plugins:{legend:{position:'right',labels:{font:{size:10},boxWidth:10}}},cutout:'60%'}});
  const bins=new Array(10).fill(0);
  ac.forEach(a=>{const sp=spoofScore(a);bins[Math.min(9,Math.floor(sp/10))]++;});
  if(ch2) ch2.destroy();
  ch2=new Chart(document.getElementById('ch-prob').getContext('2d'),{
    type:'bar',data:{labels:['0-10','10-20','20-30','30-40','40-50','50-60','60-70','70-80','80-90','90-100'],
    datasets:[{data:bins,backgroundColor:bins.map((_,i)=>i>=6?'#ff2d55':i>=3?'#f97316':'#2fe6a7'),borderRadius:2,borderSkipped:false}]},
    options:{plugins:{legend:{display:false}},scales:{x:{ticks:{font:{size:9}}},y:{beginAtZero:true}}}});
  const tbody=document.getElementById('stb');
  const rows=[...ac].map(a=>({...a,sp:spoofScore(a)})).filter(a=>a.sp>0).sort((a,b)=>b.sp-a.sp).slice(0,50);
  if(!rows.length){tbody.innerHTML='<tr><td colspan="9" class="td-dim">No suspicious aircraft detected.</td></tr>';return;}
  tbody.innerHTML=rows.map(a=>{
    const cls=checkMil(a)?'CONFIRMED_MILITARY':(a.cls||'UNKNOWN');
    const cb={CIVILIAN:'civil',LIKELY_MILITARY:'likely',CONFIRMED_MILITARY:'mil',DARK_AIRCRAFT:'dark',UNKNOWN:'dark'}[cls]||'civil';
    const sb=a.sp>=60?'spoof':a.sp>=30?'sus':'clean';
    const bc=a.sp>=60?'#ff2d55':a.sp>=30?'#f97316':'#2fe6a7';
    const evts=acEvents[a.icao]||[];
    const anomalyText=a.anoms?.join(', ').replace(/_/g,' ')||'--';
    return '<tr style="cursor:pointer" data-open-panel="'+esc(a.icao)+'" data-switch-tab="map">'+
      '<td style="font-weight:600;font-family:var(--mono)">'+esc(a.icao)+'</td>'+
      '<td>'+esc(a.cs||'--')+'</td>'+
      '<td>'+(a.lat!=null?a.lat.toFixed(4):'--')+'</td>'+
      '<td>'+(a.lon!=null?a.lon.toFixed(4):'--')+'</td>'+
      '<td>'+(a.alt!=null?a.alt.toLocaleString():'--')+'</td>'+
      '<td>'+(a.vel!=null?Math.round(a.vel):'--')+'</td>'+
      '<td><span class="badge '+cb+'">'+esc(cls.replace(/_/g,' '))+'</span></td>'+
      '<td><div class="pb"><div class="pb-bar"><div class="pb-fill" style="width:'+a.sp+'%;background:'+bc+'"></div></div><span class="badge '+sb+'">'+a.sp+'%</span></div></td>'+
      '<td style="font-size:11px;color:var(--text2);max-width:160px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+(evts.length?evts.length+' events | ':'')+esc(anomalyText)+'</td>'+
    '</tr>';
  }).join('');
}

// ── AI Analysis ───────────────────────────────────────────────
function handleCSV(input){
  const file=input.files[0]; if(!file) return;
  const reader=new FileReader();
  reader.onload=e=>{
    const lines=e.target.result.trim().split('\n');
    csvHeaders=lines[0].split(',').map(h=>h.trim());
    csvRows=lines.slice(1).map(l=>{
      const vals=l.split(','),obj={};
      csvHeaders.forEach((h,i)=>obj[h]=(vals[i]||'').trim());
      return obj;
    }).filter(r=>r[csvHeaders[0]]);
    const sp=csvRows.filter(r=>r.IsSpoofed==='1').length;
    const b=document.getElementById('ai-banner');
    b.style.display='block'; b.className='banner green';
    b.textContent='Loaded '+csvRows.length.toLocaleString()+' rows — '+sp.toLocaleString()+' spoofed, '+(csvRows.length-sp).toLocaleString()+' clean — '+file.name;
    document.getElementById('ai-preview').style.display='block';
    document.getElementById('ai-row-count').textContent='(first 20 rows shown)';
    document.getElementById('ai-thead').innerHTML='<tr>'+csvHeaders.map(h=>'<th>'+esc(h)+'</th>').join('')+'</tr>';
    document.getElementById('ai-tbody').innerHTML=csvRows.slice(0,20).map(r=>
      '<tr>'+csvHeaders.map(h=>'<td style="color:'+(r.IsSpoofed==='1'?'#ff2d55':'var(--text)')+'">'+esc(r[h]||'--')+'</td>').join('')+'</tr>'
    ).join('');
    document.getElementById('ai-btn').style.display='inline-block';
    buildAICharts(csvRows);
  };
  reader.readAsText(file);
}

function buildAICharts(rows){
  const spoofed=rows.filter(r=>r.IsSpoofed==='1');
  const reasons={'Invalid coordinates':0,'Impossible altitude':0,'Impossible speed':0,'Malformed message':0,'Negative speed':0,'Other':0};
  spoofed.forEach(r=>{
    const lat=parseFloat(r.Latitude),lon=parseFloat(r.Longitude),alt=parseFloat(r.Altitude_ft),spd=parseFloat(r.Speed_knots);
    if(isNaN(lat)||lat<-90||lat>90||isNaN(lon)||lon<-180||lon>180) reasons['Invalid coordinates']++;
    else if(alt===999999||alt>90000) reasons['Impossible altitude']++;
    else if(spd>1200) reasons['Impossible speed']++;
    else if(spd<0)    reasons['Negative speed']++;
    else if((r.MessageContent||'').includes('MALFORMED')) reasons['Malformed message']++;
    else reasons['Other']++;
  });
  if(aiRC) aiRC.destroy();
  aiRC=new Chart(document.getElementById('ai-rc').getContext('2d'),{
    type:'doughnut',data:{labels:Object.keys(reasons),datasets:[{data:Object.values(reasons),backgroundColor:['#ff2d55','#f97316','#a855f7','#22d3ee','#ffb020','#5c6b84'],borderWidth:0}]},
    options:{plugins:{legend:{position:'right',labels:{font:{size:10},boxWidth:10}}},cutout:'55%',maintainAspectRatio:false}});
  const bins=new Array(12).fill(0);
  spoofed.forEach(r=>{const s=parseFloat(r.Speed_knots);if(!isNaN(s)&&s>=0) bins[Math.min(11,Math.floor(s/100))]++;});
  if(aiSC) aiSC.destroy();
  aiSC=new Chart(document.getElementById('ai-sc').getContext('2d'),{
    type:'bar',data:{labels:['0-100','100-200','200-300','300-400','400-500','500-600','600-700','700-800','800-900','900-1000','1000-1100','1100+'],
    datasets:[{data:bins,backgroundColor:bins.map((_,i)=>i>=10?'#ff2d55':i>=8?'#f97316':'#22d3ee'),borderRadius:2,borderSkipped:false}]},
    options:{plugins:{legend:{display:false}},scales:{x:{ticks:{font:{size:8},maxRotation:45}},y:{beginAtZero:true}},maintainAspectRatio:false}});
  const tot=rows.length,nSp=spoofed.length;
  const msgTypes={};
  spoofed.forEach(r=>{msgTypes[r.MessageType]=(msgTypes[r.MessageType]||0)+1;});
  const topMsg=Object.entries(msgTypes).sort((a,b)=>b[1]-a[1])[0];
  document.getElementById('ai-stat-grid').innerHTML=[
    ['Total records',tot.toLocaleString(),'var(--text)'],
    ['Labeled suspicious',nSp.toLocaleString(),'#ff2d55'],
    ['Label rate',((nSp/tot)*100).toFixed(1)+'%','#f97316'],
    ['Top msg type',topMsg?topMsg[0]:'--','#a855f7'],
  ].map(([l,v,c])=>'<div class="sc"><div class="v" style="color:'+c+'">'+esc(v)+'</div><div class="l">'+esc(l)+'</div></div>').join('');
}

async function runAIAnalysis(){
  document.getElementById('ai-btn').style.display='none';
  document.getElementById('ai-loading').style.display='block';
  document.getElementById('ai-results').style.display='none';
  const setP=t=>document.getElementById('ai-progress').textContent=t;
  try{
    setP('Sampling dataset...');
    const spoofed=csvRows.filter(r=>r.IsSpoofed==='1').slice(0,40);
    const tot=csvRows.length,nSp=csvRows.filter(r=>r.IsSpoofed==='1').length;
    const reasons={};
    spoofed.forEach(r=>{
      const lat=parseFloat(r.Latitude),lon=parseFloat(r.Longitude),alt=parseFloat(r.Altitude_ft),spd=parseFloat(r.Speed_knots);
      let reason='Unknown';
      if(isNaN(lat)||lat<-90||lat>90||isNaN(lon)||lon<-180||lon>180) reason='Invalid coordinates';
      else if(alt===999999||alt>90000) reason='Impossible altitude';
      else if(spd>1200) reason='Impossible speed';
      else if(spd<0)    reason='Negative speed';
      else if((r.MessageContent||'').includes('MALFORMED')) reason='Malformed message';
      reasons[reason]=(reasons[reason]||0)+1;
    });
    setP('Generating local heuristic summary...');
    const reasonSummary=Object.entries(reasons)
      .sort((a,b)=>b[1]-a[1])
      .map(([reason,count])=>`${reason}: ${count}`)
      .join(', ') || 'No labeled anomalies';
    const narrative=`This analysis runs locally in your browser; no uploaded telemetry is sent to a third party. The dataset contains ${tot.toLocaleString()} records, of which ${nSp.toLocaleString()} are labeled suspicious (${((nSp/tot)*100).toFixed(1)}%).\n\nThe dominant label-derived reasons are: ${reasonSummary}. These are deterministic rule matches against the CSV's existing IsSpoofed labels, not independent confirmation of an attack.\n\nTreat the results as dataset triage only. Confirm suspicious records against independent receiver timing, source provenance, and known-good ground truth before making an operational claim.`;
    setP('Building per-aircraft breakdown...');
    const explained=spoofed.slice(0,100).map(r=>{
      const lat=parseFloat(r.Latitude),lon=parseFloat(r.Longitude),alt=parseFloat(r.Altitude_ft),spd=parseFloat(r.Speed_knots);
      let reason='',conf=0;
      if(isNaN(lat)||lat<-90||lat>90){reason='Latitude '+r.Latitude+' outside valid range (-90 to 90)';conf=98;}
      else if(isNaN(lon)||lon<-180||lon>180){reason='Longitude '+r.Longitude+' outside valid range (-180 to 180)';conf=98;}
      else if(alt===999999){reason='Altitude 999,999 ft — sentinel value used in spoofed datasets';conf=97;}
      else if(alt>90000){reason='Altitude '+alt.toLocaleString()+' ft exceeds stratosphere limit';conf=94;}
      else if(spd>1200){reason='Speed '+spd+' kts exceeds physical maximum for any aircraft';conf=96;}
      else if(spd<0){reason='Speed '+spd+' kts is negative — physically impossible';conf=99;}
      else if((r.MessageContent||'').includes('MALFORMED')){reason='Message content flagged as malformed: '+r.MessageContent;conf=92;}
      else{reason='Anomalous telemetry combination';conf=71;}
      return{...r,_reason:reason,_conf:conf};
    });
    document.getElementById('ai-loading').style.display='none';
    document.getElementById('ai-results').style.display='block';
    document.getElementById('ai-narrative').textContent=narrative;
    buildAICharts(csvRows);
    document.getElementById('ai-table').innerHTML=explained.map(r=>
      '<tr>'+
      '<td style="font-weight:600;font-family:var(--mono);font-size:11px;color:#ff2d55">'+esc(r.AircraftID)+'</td>'+
      '<td>'+esc(r.FlightNumber||'--')+'</td>'+
      '<td style="font-family:var(--mono);font-size:11px">'+esc(r.Latitude||'--')+' / '+esc(r.Longitude||'--')+'</td>'+
      '<td style="color:'+(r.Altitude_ft==='999999'?'#ff2d55':'var(--text)')+'">'+esc(r.Altitude_ft||'--')+'</td>'+
      '<td style="color:'+(parseFloat(r.Speed_knots)<0?'#ff2d55':'var(--text)')+'">'+esc(r.Speed_knots||'--')+'</td>'+
      '<td style="font-size:11px;color:var(--text2);max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+esc(r.MessageContent||'--')+'</td>'+
      '<td style="font-size:11px;color:#ff2d55;max-width:200px">'+esc(r._reason)+'</td>'+
      '<td><div class="pb"><div class="pb-bar"><div class="pb-fill" style="width:'+r._conf+'%;background:'+(r._conf>90?'#ff2d55':'#f97316')+'"></div></div><b style="font-size:11px;color:'+(r._conf>90?'#ff2d55':'#f97316')+'">'+r._conf+'%</b></div></td>'+
      '</tr>'
    ).join('');
  } catch(err){
    document.getElementById('ai-loading').style.display='none';
    document.getElementById('ai-btn').style.display='inline-block';
    const b=document.getElementById('ai-banner');
    b.className='banner red'; b.textContent='Error: '+err.message;
  }
}

// ── Manual entry ──────────────────────────────────────────────
function analyzeManual(){
  const a={icao:document.getElementById('m-icao').value.trim().toUpperCase(),
    cs:document.getElementById('m-cs').value.trim(),
    lat:parseFloat(document.getElementById('m-lat').value),
    lon:parseFloat(document.getElementById('m-lon').value),
    alt:parseFloat(document.getElementById('m-alt').value),
    vel:parseFloat(document.getElementById('m-vel').value),
    hdg:parseFloat(document.getElementById('m-hdg').value),
    vr:parseFloat(document.getElementById('m-vr').value),
    risk:0,anoms:[],cls:'UNKNOWN'};
  const sp=spoofScore(a),mil=checkMil(a);
  const cls=mil?'CONFIRMED MILITARY':sp>=60?'HEURISTIC ALERT':sp>=30?'SUSPICIOUS':'CIVILIAN';
  const col=sp>=60?'#ff2d55':sp>=30?'#f97316':'#2fe6a7';
  const reasons=[];
  if(a.lat<-90||a.lat>90)   reasons.push('Latitude out of valid range (-90 to 90)');
  if(a.lon<-180||a.lon>180) reasons.push('Longitude out of valid range (-180 to 180)');
  if(a.alt===999999)        reasons.push('Sentinel altitude value 999999');
  if(a.vel===9999)          reasons.push('Sentinel speed value 9999');
  if(a.vel>1200)            reasons.push('Speed '+a.vel+' kts exceeds physical maximum');
  if(a.alt>90000)           reasons.push('Altitude '+a.alt.toLocaleString()+' ft above stratosphere');
  if(mil)                   reasons.push('ICAO24 in confirmed military address block');
  const box=document.getElementById('mres'); box.style.display='block';
  box.innerHTML='<h3>Result — '+esc(a.icao||'?')+'</h3>'+
    '<div class="rr"><span class="k">Spoof Probability</span><b style="color:'+col+';font-size:20px">'+sp+'%</b></div>'+
    '<div class="rr"><span class="k">Classification</span><b>'+cls+'</b></div>'+
    '<div class="rr"><span class="k">Callsign</span><span>'+esc(a.cs||'--')+'</span></div>'+
    '<div class="rr"><span class="k">Position</span><span>'+(isNaN(a.lat)?'--':a.lat.toFixed(4))+', '+(isNaN(a.lon)?'--':a.lon.toFixed(4))+'</span></div>'+
    '<div class="rr"><span class="k">Altitude</span><span>'+(isNaN(a.alt)?'--':a.alt.toLocaleString()+' ft')+'</span></div>'+
    '<div class="rr"><span class="k">Speed</span><span>'+(isNaN(a.vel)?'--':a.vel+' kts')+'</span></div>'+
    (reasons.length?'<div class="rr" style="flex-direction:column;gap:4px"><span class="k">Detection reasons</span>'+
      reasons.map(r=>'<span style="color:#ef4444;font-size:12px">+ '+esc(r)+'</span>').join('')+'</div>':
    '<div class="rr"><span class="k">Detection reasons</span><span style="color:#2fe6a7">No anomalies detected</span></div>');
}

// ── WebSocket + REST polling ──────────────────────────────────
let wsConn=null;

// ── Multi-source aircraft data ────────────────────────────────
// Priority order:
//   1. Docker backend WebSocket (localhost:8000) — highest fidelity, fused data
//   2. Docker backend REST poll — fallback if WS drops
//   3. api.adsb.fi — CORS-enabled, free, ~10,000+ aircraft, no auth needed
//   4. OpenSky Network — CORS-enabled, free, rate-limited fallback
//
// Sources 3 and 4 work even when Docker is completely off.
// The merged deduplicated result is displayed on the map.

let _backendAlive = false;           // true once localhost:8000 responds
let _directCache  = {};              // icao → aircraft (from direct sources)
let _backendCache = {};              // icao → aircraft (from backend)

// Normalise any raw aircraft object into our internal format
function normalise(raw, src) {
  const lat = parseFloat(raw.lat ?? raw.latitude  ?? raw.Latitude);
  const lon = parseFloat(raw.lon ?? raw.longitude ?? raw.Longitude ?? raw.lng);
  if (isNaN(lat) || isNaN(lon)) return null;
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;

  const icao = (raw.hex ?? raw.icao ?? raw.icao24 ?? raw.AircraftID ?? '').toUpperCase().trim();
  if (!/^[0-9A-F]{6}$/.test(icao)) return null;

  const altRaw = raw.alt_baro ?? raw.altitude ?? raw.baro_altitude ?? raw.Altitude_ft;
  const alt    = altRaw === 'ground' || altRaw === 'grnd' ? 0
               : altRaw != null ? parseInt(altRaw) : null;

  const velRaw = raw.gs ?? raw.velocity ?? raw.speed ?? raw.Speed_knots;
  const vel    = velRaw != null ? Math.round(parseFloat(velRaw) * (src === 'opensky' ? 1.944 : 1)) : null;

  const hdg    = raw.track ?? raw.heading ?? raw.true_track ?? null;
  const vr     = raw.baro_rate ?? raw.vertical_rate ?? null;
  const cs     = (raw.flight ?? raw.callsign ?? raw.FlightNumber ?? '').trim() || null;
  const sqk    = (raw.sqk ?? raw.squawk ?? '').toString().trim() || null;

  return {
    icao, cs, sqk,
    lat, lon,
    alt: alt != null ? parseInt(alt) : null,
    vel: vel != null ? parseFloat(vel) : null,
    hdg: hdg != null ? parseFloat(hdg) : null,
    vr:  vr  != null ? parseInt(vr)   : null,
    on_ground: raw.on_ground === true || altRaw === 'ground',
    risk: 0, anoms: [], cls: 'CIVILIAN', src, conf: 0.8, mil: 0, band: 'NORMAL', trail: []
  };
}

// ── Source status tracking ────────────────────────────────────
function updateSourceStatus(key, count, ok) {
  const el = document.getElementById('src-' + key);
  if (!el) return;
  el.className = 'src ' + (ok ? 'ok' : 'bad');
  const label=({ backend: 'Docker backend', live: 'Server live feed', conflicts: 'Passive conflicts' }[key] || key);
  const unit=key==='conflicts' ? ' projections' : ' aircraft';
  el.textContent=label+': '+(ok ? count.toLocaleString()+unit : 'failed');
}

function conflictLineEndpoints(first,second){
  const firstLat=Number(first.lat),firstLon=Number(first.lon);
  const secondLat=Number(second.lat); let secondLon=Number(second.lon);
  const delta=secondLon-firstLon;
  if(delta>180) secondLon-=360;
  else if(delta<-180) secondLon+=360;
  return [[firstLat,firstLon],[secondLat,secondLon]];
}

function renderConflicts(){
  if(!conflictLayerGroup) return;
  conflictLayerGroup.clearLayers();
  const conflicts=Array.isArray(_conflictAnalysis.conflicts) ? _conflictAnalysis.conflicts : [];
  const counter=document.getElementById('cconf');
  if(counter) counter.textContent=conflicts.length.toLocaleString();
  const toggle=document.getElementById('chk-conflicts');
  if(toggle&&!toggle.checked) return;
  conflicts.slice(0,500).forEach(conflict=>{
    const pair=Array.isArray(conflict.pair) ? conflict.pair : [];
    const first=_backendCache[pair[0]],second=_backendCache[pair[1]];
    if(!first||!second||!Number.isFinite(Number(first.lat))||!Number.isFinite(Number(first.lon))||
       !Number.isFinite(Number(second.lat))||!Number.isFinite(Number(second.lon))) return;
    const critical=conflict.severity==='PREDICTED_LOSS_OF_SEPARATION';
    L.polyline(conflictLineEndpoints(first,second),{
      color:critical?'#ff2d55':'#ffb020',weight:critical?3:2,dashArray:'6 5',opacity:0.9,interactive:false
    }).addTo(conflictLayerGroup);
  });
}

function mergeAndRender() {
  const merged = { ..._directCache };
  Object.assign(merged, _backendCache);
  const aircraft = Object.values(merged).filter(insideCoverage);
  renderAircraft(aircraft);
  renderConflicts();
  updateEDA(aircraft);
  const b = document.getElementById('banner');
  b.className   = 'banner green';
  b.textContent = 'Live — ' + aircraft.length.toLocaleString() + ' aircraft tracked in current area';
}

// ── WebSocket (for anomaly scores from fusion pipeline) ───────
function connectWS() {
  setWS('connecting');
  wsConn = new WebSocket(WS_URL);
  wsConn.binaryType = 'arraybuffer';
  wsConn.onopen  = () => { setWS('connected'); _backendAlive = true; };
  wsConn.onmessage = e => {
    try {
      const t = e.data instanceof ArrayBuffer ? new TextDecoder().decode(e.data) : e.data;
      if (t === 'ping') { wsConn.send('pong'); return; }
      const m = JSON.parse(t);
      if (m.type === 'snapshot' && Array.isArray(m.aircraft)) {
        _backendCache = {};
        m.aircraft.forEach(a => { if (a.icao) _backendCache[a.icao] = a; });
        _conflictAnalysis=m.conflict_analysis&&typeof m.conflict_analysis==='object'
          ? m.conflict_analysis : {conflicts:[]};
        updateSourceStatus('backend', m.aircraft.length, true);
        const conflictReady=_conflictAnalysis.analysis_available!==false;
        updateSourceStatus('conflicts',conflictReady?
          (Array.isArray(_conflictAnalysis.conflicts)?_conflictAnalysis.conflicts.length:0):0,conflictReady);
        mergeAndRender();
      }
    } catch (err) {}
  };
  wsConn.onclose = () => {
    setWS('disconnected');
    _backendAlive = false;
    _backendCache = {};
    _conflictAnalysis={conflicts:[]};
    updateSourceStatus('conflicts', 0, false);
    mergeAndRender();
    setTimeout(connectWS, 5000);
  };
  wsConn.onerror = () => { _backendAlive = false; };
}

// ── REST poll — hits /api/live-aircraft (server-side adsb.lol proxy) ─────────
async function pollLive() {
  try {
    const ctrl  = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 25000);
    const r = await fetch(API + '/api/live-aircraft', { signal: ctrl.signal });
    clearTimeout(timer);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    if (Array.isArray(d.aircraft)) {
      // Replace the direct feed snapshot so a live area switch drops old-region tracks.
      _directCache = {};
      d.aircraft.forEach(a => {
        if (a.icao) _directCache[a.icao] = a;
      });
      updateSourceStatus('live', d.count, true);
      mergeAndRender();
    }
  } catch (e) {
    updateSourceStatus('live', 0, false);
    console.warn('[live-aircraft] poll failed:', e.message);
  }
}

function setWS(s) {
  document.getElementById('wsd').className   = 'wsd ' + s;
  document.getElementById('wslbl').textContent = {
    connected:    'Live',
    connecting:   'Connecting...',
    disconnected: 'Reconnecting...'
  }[s];
}

function switchTab(name) {
  document.querySelectorAll('.tc').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  const tab=document.querySelector('.tab[data-tab="'+name+'"]');
  if(tab) tab.classList.add('active');
  if (name === 'map') setTimeout(() => map.invalidateSize(), 50);
}

function tickClock(){
  const el=document.getElementById('utc-clock');
  if(el) el.textContent=new Date().toISOString().substr(11,8)+' UTC';
}

function wireControls(){
  document.querySelectorAll('.tab').forEach(tab=>
    tab.addEventListener('click',()=>switchTab(tab.dataset.tab)));
  document.getElementById('hp-close').addEventListener('click',closePanel);
  document.getElementById('chk-spoof').addEventListener('change',applyFilters);
  document.getElementById('chk-mil').addEventListener('change',applyFilters);
  document.getElementById('chk-apt').addEventListener('change',drawAirports);
  document.getElementById('chk-conflicts').addEventListener('change',renderConflicts);
  document.getElementById('watch-ghosts').addEventListener('change',renderWatchGhosts);
  document.getElementById('apt-sel').addEventListener('change',flyTo);
  document.getElementById('coverage-airport').addEventListener('change',selectCoverageAirport);
  document.getElementById('coverage-map-center').addEventListener('click',useMapCenterForCoverage);
  document.getElementById('coverage-apply').addEventListener('click',applyCoverage);
  document.getElementById('world-scan-toggle').addEventListener('click',updateWorldScan);
  document.getElementById('history-window').addEventListener('change',loadHistoricalAnomalies);
  document.getElementById('history-markers').addEventListener('change',()=>renderHistoricalAnomalies());
  document.getElementById('hotspot-layer').addEventListener('change',()=>renderHotspots());
  document.getElementById('layer-select').addEventListener('change',updateLayerTriggers);
  document.getElementById('ai-dropzone').addEventListener('click',()=>document.getElementById('ai-file').click());
  document.getElementById('ai-file').addEventListener('change',function(){handleCSV(this);});
  document.getElementById('ai-btn').addEventListener('click',runAIAnalysis);
  document.getElementById('m-analyze').addEventListener('click',analyzeManual);
  document.getElementById('watch-hours').addEventListener('change',loadWatchEvents);
  document.querySelectorAll('.wfilter').forEach(button=>
    button.addEventListener('click',()=>{
      document.querySelectorAll('.wfilter').forEach(b=>b.classList.remove('active'));
      button.classList.add('active');
      watchKindFilter=button.dataset.wkind;
      loadWatchEvents();
    }));
}

window.addEventListener('DOMContentLoaded', () => {
  wireControls();
  document.addEventListener('click', event => {
    const watchTarget=event.target.closest('[data-watch-lat]');
    if(watchTarget&&watchTarget.dataset.watchLat){
      switchTab('map');
      map.flyTo([Number(watchTarget.dataset.watchLat),Number(watchTarget.dataset.watchLon)],8,{duration:0.8});
      return;
    }
    const detailTarget=event.target.closest('[data-aircraft-details]');
    if(detailTarget){
      if(detailTarget.dataset.closePopup==='1') map.closePopup();
      switchTab('map');
      openAircraftDetails(detailTarget.dataset.aircraftDetails);
      return;
    }
    const panelTarget=event.target.closest('[data-open-panel]');
    if(!panelTarget) return;
    if(panelTarget.dataset.closePopup==='1') map.closePopup();
    openPanel(panelTarget.dataset.openPanel);
    if(panelTarget.dataset.switchTab) switchTab(panelTarget.dataset.switchTab);
  });
  initMap();
  loadCoverage();
  loadWorldScan();
  loadHistoricalAnomalies();

  // WebSocket for real-time anomaly-scored data
  connectWS();

  // Poll /api/live-aircraft every 12s — server proxies adsb.lol, no CORS
  pollLive();
  setInterval(pollLive, 12000);

  loadWatchSummary();
  loadWatchEvents();
  setInterval(()=>{ loadWatchSummary(); loadWatchEvents(); }, 20000);

  updateLayerSummary();
  updateLayerTriggers();
  setInterval(()=>{ updateLayerSummary(); updateLayerTriggers(); }, 15000);
  setInterval(()=>{ loadWorldScan(); loadCoverage(); }, 30000);
  setInterval(loadHistoricalAnomalies, 60000);
  tickClock();
  setInterval(tickClock, 1000);
});
