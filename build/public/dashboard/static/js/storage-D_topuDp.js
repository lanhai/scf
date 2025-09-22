/*!  build: Vue Shop Vite 
     copyright: https://vuejs-core.cn/shop-vite   
     time: 2025-09-22 17:16:29 
 */
const l="https://lky-chengdu.oss-cn-chengdu.aliyuncs.com/dashboard/static/png/left_img_1-Dp36PNG8.png",r={set(t,e){let a=typeof e;localStorage.setItem(t,JSON.stringify({type:a,data:e}))},get(t){const e=localStorage.getItem(t);if(!e)return null;let a=JSON.parse(e);return a?a.data:null},delete(t){return localStorage.removeItem(t)},clear(){return localStorage.clear()}};export{r as $,l};
