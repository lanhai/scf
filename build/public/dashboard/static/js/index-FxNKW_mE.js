/*!  build: Vue Shop Vite 
     copyright: https://vuejs-core.cn/shop-vite   
     time: 2025-09-23 14:06:32 
 */
import{P as i}from"./index.min-DllbqQmz.js";import{d as s,r as c,w as l,C as u,ab as f,a as d,o as p}from"./vsv-element-plus-Cu8LirE2.js";const m=["id"],P=s({name:"VabPlayer",__name:"index",props:{config:{type:Object,default:()=>({id:"mse",url:""})}},emits:["player"],setup(t,{emit:a}){const o=t,e=c(null),r=a,n=()=>{o.config.url&&o.config.url!==""&&(e.value=new i(o.config),r("player",e.value))};return l(o.config,()=>{n()},{deep:!0}),u(()=>{n()}),f(()=>{e.value&&typeof e.value.destroy=="function"&&e.value.destroy()}),(y,g)=>(p(),d("div",{id:t.config.id},null,8,m))}});export{P as default};
