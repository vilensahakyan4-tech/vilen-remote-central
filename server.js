const http=require('http');
const crypto=require('crypto');
const PORT=process.env.PORT||10000;
const streams=new Map();
const pendingEvents=new Map();
const devices=new Map();
let droppedFrames=0;
const FRAME_INTERVAL=400;
function send(res,status,body){res.writeHead(status,{'content-type':'application/json; charset=utf-8','access-control-allow-origin':'*','cache-control':'no-store'});res.end(JSON.stringify(body));}
function body(req){return new Promise((ok,no)=>{let d='';req.on('data',c=>{d+=c;if(d.length>1e6)req.destroy();});req.on('end',()=>{try{ok(JSON.parse(d||'{}'));}catch(e){no(e);}});});}
function queueEvent(key,event){
  if(event?.type==='fallback-frame')return;
  const queue=pendingEvents.get(key)||[];
  queue.push({event,expires:Date.now()+60000});
  pendingEvents.set(key,queue.slice(-128));
}
function scheduleLatestFrame(client){
  if(client.closed||client.frameTimer||!client.latestFrame||client.backpressured)return;
  const wait=Math.max(0,FRAME_INTERVAL-(Date.now()-client.lastFrameAt));
  client.frameTimer=setTimeout(()=>{client.frameTimer=null;const latest=client.latestFrame;client.latestFrame=null;if(latest)writeStream(client,latest);},wait);
}
function writeStream(client,event){
  const isFrame=event?.type==='fallback-frame';
  if(isFrame){
    const wait=FRAME_INTERVAL-(Date.now()-client.lastFrameAt);
    if(client.backpressured||wait>0){if(client.latestFrame)droppedFrames++;client.latestFrame=event;scheduleLatestFrame(client);return;}
    client.lastFrameAt=Date.now();
  }
  const writable=client.res.write(`data: ${JSON.stringify(event)}\n\n`);
  if(isFrame&&!writable){client.backpressured=true;client.res.once('drain',()=>{client.backpressured=false;scheduleLatestFrame(client);});}
}
function emit(key,event,durable=false){
  const receivers=streams.get(key);
  if(receivers?.size)for(const client of receivers)writeStream(client,event);
  if(!receivers?.size||durable)queueEvent(key,event);
}
function takePending(key){
  const now=Date.now(),queue=pendingEvents.get(key)||[],events=queue.filter(item=>item.expires>now).map(item=>item.event);
  pendingEvents.delete(key);return events;
}
function flush(key,client){
  for(const event of takePending(key))writeStream(client,event);
}
function safeId(id){return /^\d{9}$/.test(String(id||''));}
setInterval(()=>{const now=Date.now();for(const[id,d]of devices)if(now-d.seen>45000)devices.delete(id);for(const[key,queue]of pendingEvents){const live=queue.filter(x=>x.expires>now);if(live.length)pendingEvents.set(key,live);else pendingEvents.delete(key);}},15000);
http.createServer(async(req,res)=>{
  res.setHeader('access-control-allow-origin','*');res.setHeader('access-control-allow-headers','content-type');
  if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
  const url=new URL(req.url,`http://${req.headers.host}`);
  if(req.method==='GET'&&url.pathname==='/health')return send(res,200,{ok:true,service:'Vilen Remote Central',online:devices.size,droppedFrames,frameInterval:FRAME_INTERVAL});
  if(req.method==='GET'&&url.pathname==='/events'){
    const key=url.searchParams.get('key');if(!key)return send(res,400,{error:'key required'});
    res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-cache','connection':'keep-alive','access-control-allow-origin':'*'});res.write(': connected\n\n');
    const client={res,backpressured:false,latestFrame:null,lastFrameAt:0,frameTimer:null,closed:false};if(!streams.has(key))streams.set(key,new Set());streams.get(key).add(client);flush(key,client);req.on('close',()=>{client.closed=true;clearTimeout(client.frameTimer);const set=streams.get(key);set?.delete(client);if(!set?.size)streams.delete(key);});return;
  }
  if(req.method==='POST'&&url.pathname==='/api/register'){
    try{const x=await body(req);if(!safeId(x.id))return send(res,400,{error:'invalid id'});devices.set(x.id,{name:String(x.name||'Computer').slice(0,80),seen:Date.now()});return send(res,200,{ok:true,events:takePending(`device:${x.id}`)});}catch{return send(res,400,{error:'bad request'});}
  }
  if(req.method==='POST'&&url.pathname==='/api/online'){
    const x=await body(req).catch(()=>({}));return send(res,200,{online:devices.has(String(x.id)),name:devices.get(String(x.id))?.name||null});
  }
  if(req.method==='POST'&&url.pathname==='/api/connect'){
    const x=await body(req).catch(()=>({}));if(!devices.has(String(x.id)))return send(res,404,{error:'Компьютер не в сети'});
    const requestId=crypto.randomUUID();emit(`device:${x.id}`,{type:'connect-request',requestId,password:String(x.password||'')},true);return send(res,200,{requestId});
  }
  if(req.method==='POST'&&url.pathname==='/api/event'){
    const x=await body(req).catch(()=>({}));if(!x.key)return send(res,400,{error:'key required'});emit(String(x.key),x.event||{});return send(res,200,{ok:true});
  }
  send(res,404,{error:'not found'});
}).listen(PORT,()=>console.log(`Vilen Remote Central on ${PORT}`));
