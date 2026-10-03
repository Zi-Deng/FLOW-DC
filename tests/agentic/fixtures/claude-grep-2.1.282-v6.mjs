import assert from 'node:assert/strict';
import {relative as v} from 'node:path';
const ne=()=>'/synthetic/workspace',oA='/',zo=JSON.parse,VCn=250,P=(n,s)=>n===1?s:s+'s';
function nRn(e,n){let s=(F)=>F===void 0?"":F.text!==void 0?F.text:Buffer.from(F.bytes??"","base64").toString("utf8"),g=(F,B)=>F.length>500?B==="match"?"[Omitted long matching line]":"[Omitted long context line]":F,h=[],b=[],w=null,M=-1;for(let F of e){if(!F.startsWith("{"))continue;let B;try{B=zo(F)}catch{continue}if(B===null||typeof B!=="object")continue;let G=B.data??{};if(B.type==="begin"){b=[];continue}if(B.type==="end"){let $e=G.binary_offset,Le=typeof $e==="number";for(let He of b)if(!Le||!He.slice(He.indexOf("\x00")+1).includes("\x00"))h.push(He);if(Le&&b.length>0)h.push(`${s(G.path)}\x00binary file matches (found "\\0" byte around offset ${$e})`);b=[];continue}if(B.type!=="match"&&B.type!=="context")continue;let ge=s(G.path),_e=G.line_number??0,we=s(G.lines).replace(/\r?\n$/,"");if(n.contextBreaks&&w!==null&&(ge!==w||_e>M+1))b.push("--");if(B.type==="match"&&n.onlyMatching){let $e=_e;for(let Le of G.submatches??[]){let He=_e+oRn(Buffer.from(we),Le.start??0);s(Le.match).split(`
`).forEach((We,Xe)=>{b.push(`${ge}\x00${He+Xe}:${g(We.replace(/\r$/,""),"match")}`),$e=He+Xe})}w=ge,M=$e;continue}let Ee=B.type==="match"?"match":"context",xe=Ee==="match"?":":"-",Ie=we.split(`
`).map(($e)=>$e.replace(/\r$/,""));Ie.forEach(($e,Le)=>{b.push(`${ge}\x00${_e+Le}${xe}${g($e,Ee)}`)}),w=ge,M=_e+Ie.length-1}for(let F of b)h.push(F);return h}function eRn(e,n){let r=n.lexical.endsWith(oA)?n.lexical:n.lexical+oA;if(n.relativeOutput)return e.startsWith("./")?r+e.slice(2):e;for(let s of[n.target,n.canonical]){let g=s.endsWith(oA)?s:s+oA;if(e.startsWith(g))return r+e.slice(g.length);if(e===s)return n.lexical;if(e.startsWith(`${s}:`)||e.startsWith(`${s}\x00`))return n.lexical+e.slice(s.length)}return e}function Bct(e){let r=v(ne(),e);return r.startsWith("..")?e:r}function V_e(e,n,r=0){if(n===0)return{items:e.slice(r),appliedLimit:void 0};let s=n??VCn,g=e.slice(r,r+s),h=e.length-r>s;return{items:g,appliedLimit:h?s:void 0}}function Y_e(e,n){let r=[];if(e!==void 0)r.push(`limit: ${e}`);if(n)r.push(`offset: ${n}`);return r.join(", ")}
const tool={mapToolResultToToolResultBlockParam({mode:e="files_with_matches",numFiles:n,filenames:r,content:s,numLines:g,numMatches:h,totalFiles:b,totalLines:w,appliedLimit:M,appliedOffset:F},B){if(e==="content"){let _e=Y_e(M,F),we=s||(F&&(w??0)>0?"No entries at this offset":"No matches found"),Ee=_e?`${we}

[Showing results with pagination = ${_e}]`:we;return{tool_use_id:B,type:"tool_result",content:Ee}}if(e==="count"){let _e=Y_e(M,F),we=h??0,Ee=n??0,xe=s||(we>0?"No entries at this offset":"No matches found"),Ie=`

Found ${we} total ${we===1?"occurrence":"occurrences"} across ${Ee} ${Ee===1?"file":"files"}.${_e?` with pagination = ${_e}`:""}`;return{tool_use_id:B,type:"tool_result",content:xe+Ie}}let G=Y_e(M,F);if(n===0)return{tool_use_id:B,type:"tool_result",content:F&&(b??0)>0?`No entries at this offset. [Showing results with pagination = ${G}]`:"No files found"};let ge=`Found ${n} ${P(n,"file")}${G?` ${G}`:""}
${r.join(`
`)}`;return{tool_use_id:B,type:"tool_result",content:ge}}};
function project(e,ft,F=true,ge=10,_e=0){const g='content';if(g==="content"){let{items:Xn,appliedLimit:mo}=V_e(ft,ge,_e),jn=Xn.map((Vt)=>{let yn=Vt.indexOf("\x00");if(yn<0)return F?Vt:Vt.replace(/^\d+[:-]/,"");let _n=Vt.substring(yn+1),mn=/^(\d+)([:-])/.exec(_n),An=mn?.[2]??":",qo=F||mn===null?_n:_n.substring(mn[0].length);return e.isDirectory?Bct(Vt.substring(0,yn))+An+qo:qo});return{data:{mode:"content",numFiles:0,filenames:[],content:jn.join(`
`),numLines:jn.length,totalLines:ft.length,...mo!==void 0&&{appliedLimit:mo},..._e>0&&{appliedOffset:_e}}}}throw Error('unsupported fixture branch')}
const path='/synthetic/workspace/capability/fixture.txt';
const rg=[{type:'begin',data:{path:{text:path}}},{type:'match',data:{path:{text:path},line_number:2,lines:{text:'CLAUDE_NATIVE_CANARY\n'},submatches:[]}},{type:'end',data:{path:{text:path}}}].map(JSON.stringify);
const lines=nRn(rg,{contextBreaks:false,onlyMatching:false});
assert.deepEqual(lines,[path+'\0'+'2:CLAUDE_NATIVE_CANARY']);
const directory={lexical:'/synthetic/workspace',canonical:'/synthetic/workspace',target:'.',relativeOutput:false,isDirectory:true};
const file={lexical:path,canonical:path,target:path,relativeOutput:false,isDirectory:false};
function render(e,F=true,limit=10,offset=0,rows=lines){return tool.mapToolResultToToolResultBlockParam(project(e,rows.map(s=>eRn(s,e)),F,limit,offset).data,'grep-call')}
const outputs={directory:render(directory),file:render(file),noNumbers:render(directory,false),paginated:render(directory,true,1,0,[...lines,...lines]),offset:render(directory,true,10,1),empty:render(directory,true,10,0,[])};
assert.equal(outputs.directory.content,'capability/fixture.txt:2:CLAUDE_NATIVE_CANARY');
assert.equal(outputs.file.content,'2:CLAUDE_NATIVE_CANARY');
assert.equal(outputs.noNumbers.content,'capability/fixture.txt:CLAUDE_NATIVE_CANARY');
assert(outputs.paginated.content.includes('[Showing results with pagination = limit: 1]'));
assert(outputs.empty.content==='No matches found');
console.log(JSON.stringify({outputs,command:{pattern:'CLAUDE_NATIVE_CANARY',path:'.',glob:'capability/fixture.txt',output_mode:'content','-n':true,head_limit:10},not_live_evidence:true}));
