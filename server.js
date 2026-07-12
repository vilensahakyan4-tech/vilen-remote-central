const http=require('http');
const crypto=require('crypto');
const PORT=process.env.PORT||10000;
const streams=new Map();
const devices=new Map();
function send(res,status,body){res.writeHead(status,{'content-type':'application/json; charset=utf-8','access-control-allow-origin':'*','cache-control':'no-store'});res.end(JSON.stringify(body));}
function body(req){return new Promise((ok,no)=>{let d='';req.on('data',c=>{d+=c;if(d.length>1e6)req.destroy();});req.on('end',()=>{try{ok(JSON.parse(d||'{}'));}catch(e){no(e);}});});}
function emit(key,event){for(const res of streams.get(key)||[])res.write(`data: ${JSON.stringify(event)}\n\n`);}
function safeId(id){return /^\d{9}$/.test(String(id||''));}
setInterval(()=>{const now=Date.now();for(const[id,d]of devices)if(now-d.seen>45000)devices.delete(id);},15000);
http.createServer(async(req,res)=>{
  res.setHeader('access-control-allow-origin','*');res.setHeader('access-control-allow-headers','content-type');
  if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
  const url=new URL(req.url,`http://${req.headers.host}`);
  if(req.method==='GET'&&url.pathname==='/health')return send(res,200,{ok:true,service:'Vilen Remote Central',online:devices.size});
  if(req.method==='GET'&&url.pathname==='/events'){
    const key=url.searchParams.get('key');if(!key)return send(res,400,{error:'key required'});
    res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-cache','connection':'keep-alive','access-control-allow-origin':'*'});res.write(': connected\n\n');
    if(!streams.has(key))streams.set(key,new Set());streams.get(key).add(res);req.on('close',()=>streams.get(key)?.delete(res));return;
  }
  if(req.method==='POST'&&url.pathname==='/api/register'){
    try{const x=await body(req);if(!safeId(x.id))return send(res,400,{error:'invalid id'});devices.set(x.id,{name:String(x.name||'Computer').slice(0,80),seen:Date.now()});return send(res,200,{ok:true});}catch{return send(res,400,{error:'bad request'});}
  }
  if(req.method==='POST'&&url.pathname==='/api/online'){
    const x=await body(req).catch(()=>({}));return send(res,200,{online:devices.has(String(x.id)),name:devices.get(String(x.id))?.name||null});
  }
  if(req.method==='POST'&&url.pathname==='/api/connect'){
    const x=await body(req).catch(()=>({}));if(!devices.has(String(x.id)))return send(res,404,{error:'Компьютер не в сети'});
    const requestId=crypto.randomUUID();emit(`device:${x.id}`,{type:'connect-request',requestId,password:String(x.password||'')});return send(res,200,{requestId});
  }
  if(req.method==='POST'&&url.pathname==='/api/event'){
    const x=await body(req).catch(()=>({}));if(!x.key)return send(res,400,{error:'key required'});emit(String(x.key),x.event||{});return send(res,200,{ok:true});
  }
  send(res,404,{error:'not found'});
}).listen(PORT,()=>console.log(`Vilen Remote Central on ${PORT}`));
