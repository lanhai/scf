/*!  build: Vue Shop Vite 
     copyright: https://vuejs-core.cn/shop-vite   
     time: 2025-12-14 12:58:15 
 */
const a={set(e,t){let r=typeof t;localStorage.setItem(e,JSON.stringify({type:r,data:t}))},get(e){const t=localStorage.getItem(e);if(!t)return null;let r=JSON.parse(t);return r?r.data:null},delete(e){return localStorage.removeItem(e)},clear(){return localStorage.clear()}};export{a as $};
