(window["webpackJsonp"]=window["webpackJsonp"]||[]).push([["chunk-7d0e23d1"],{5231:function(t,e,a){"use strict";a.r(e);var s=function(){var t=this,e=t.$createElement,a=t._self._c||e;return a("div",[a("el-form",{ref:"searchForm",staticClass:"demo-form-inline",attrs:{inline:!0,model:t.searchParams,"label-width":"90px"}},[a("el-select",{attrs:{placeholder:"状态"},on:{change:t.getList},model:{value:t.searchParams.status,callback:function(e){t.$set(t.searchParams,"status",e)},expression:"searchParams.status"}},t._l(t.statusMaps,(function(t){return a("el-option",{key:t.status,attrs:{label:t.label,value:t.status}})})),1),a("el-form-item",{staticClass:"ml-10",attrs:{prop:"day"}},[a("el-date-picker",{attrs:{align:"right",type:"date",placeholder:"选择日期","picker-options":t.pickerOptions,format:"yyyy-MM-dd","value-format":"yyyy-MM-dd"},on:{change:t.getList},model:{value:t.searchParams.day,callback:function(e){t.$set(t.searchParams,"day",e)},expression:"searchParams.day"}})],1),a("el-form-item",[a("el-button",{attrs:{icon:"el-icon-search",type:"primary",loading:t.isLoading},on:{click:function(e){return t.getList()}}},[t._v("查询")])],1)],1),a("el-table",{staticStyle:{width:"100%"},attrs:{data:t.dataList,stripe:"","row-key":"id"}},[a("el-table-column",{attrs:{prop:"id",label:"ID",width:"240"}}),a("el-table-column",{attrs:{prop:"handler",label:"脚本",width:"200"}}),a("el-table-column",{attrs:{prop:"created",label:"创建时间"},scopedSlots:t._u([{key:"default",fn:function(e){return[t._v(t._s(t._f("HumanizedTime")(e.row.created)))]}}])}),a("el-table-column",{attrs:{prop:"finished",label:"更新时间"},scopedSlots:t._u([{key:"default",fn:function(e){return[t._v(t._s(t._f("HumanizedTime")(e.row.updated)))]}}])}),a("el-table-column",{attrs:{prop:"try_times",label:"执行次数"},scopedSlots:t._u([{key:"default",fn:function(e){return[t._v(t._s(e.row.try_times)+"/"+t._s(e.row.try_limit))]}}])}),a("el-table-column",{attrs:{prop:"status",label:"执行状态"},scopedSlots:t._u([{key:"default",fn:function(e){return[1===e.row.status?a("el-tag",{staticClass:"el-icon-success",attrs:{type:"success"}},[t._v(" 成功")]):-1===e.row.status&&e.row.try_times>=e.row.try_limit?a("el-tag",{staticClass:"el-icon-warning",attrs:{type:"warning"}},[t._v(" 失败")]):-1===e.row.status&&e.row.try_times<e.row.try_limit?a("el-tag",{staticClass:"el-icon-info",attrs:{type:"warning"}},[t._v(t._s(t._f("HumanizedTime")(e.row.next_try))+"重试")]):a("el-tag",{staticClass:"el-icon-info",attrs:{type:"info"}},[t._v(" 待执行")])]}}])}),a("el-table-column",{attrs:{prop:"try_times",label:"执行耗时"},scopedSlots:t._u([{key:"default",fn:function(e){return[t._v(t._s(e.row.duration)+"ms")]}}])}),a("el-table-column",{attrs:{prop:"biz_data",label:"业务参数"},scopedSlots:t._u([{key:"default",fn:function(e){return[a("el-popover",{attrs:{placement:"right-start",title:"业务参数",width:"600",trigger:"click"}},[a("el-row",[a("json-viewer",{attrs:{value:e.row.data,"expand-depth":10,expanded:""}})],1),a("el-button",{staticClass:"c-blue",attrs:{slot:"reference",type:"text",icon:"el-icon-document"},slot:"reference"},[t._v("查看")])],1)]}}])}),a("el-table-column",{attrs:{prop:"remark",label:"执行结果"},scopedSlots:t._u([{key:"default",fn:function(e){return[0===e.row.status?a("span",[t._v("--")]):t._e(),0!==e.row.status?a("el-popover",{attrs:{placement:"right-start",title:"执行结果",width:"600",trigger:"click"}},[-1===e.row.status?a("span",[t._v(t._s(e.row.remark))]):t._e(),a("el-row",[a("json-viewer",{attrs:{value:e.row.result,"expand-depth":10,expanded:""}})],1),a("el-button",{staticClass:"c-blue",attrs:{slot:"reference",type:"text",icon:"el-icon-document"},slot:"reference"},[t._v("查看")])],1):t._e()]}}])})],1),a("el-pagination",{attrs:{background:"","current-page":t.searchParams.page,"page-sizes":[50,100,200,500,1e3],"page-size":t.searchParams.size,layout:"total,sizes, prev, pager, next",total:t.searchParams.count},on:{"size-change":t.handleSizeChange,"current-change":t.handleCurrentChange,"update:currentPage":function(e){return t.$set(t.searchParams,"page",e)},"update:current-page":function(e){return t.$set(t.searchParams,"page",e)}}})],1)},r=[],n=(a("78ce"),a("c1df")),i=a.n(n),o=a("349e"),l=a.n(o),c={name:"Rq",components:{JsonViewer:l.a},mounted:function(){this.getList()},data:function(){return{isLoading:!1,statusMaps:[{label:"队列中",status:0},{label:"执行成功",status:1},{label:"待重试",status:2},{label:"执行失败",status:-1}],pickerOptions:{disabledDate:function(t){return t.getTime()>Date.now()},shortcuts:[{text:"今天",onClick:function(t){t.$emit("pick",new Date)}},{text:"昨天",onClick:function(t){var e=new Date;e.setTime(e.getTime()-864e5),t.$emit("pick",e)}},{text:"一周前",onClick:function(t){var e=new Date;e.setTime(e.getTime()-6048e5),t.$emit("pick",e)}}]},searchParams:{page:1,count:0,size:100,day:i()().format("YYYY-MM-DD"),status:this.$route.params.status||0},dataList:[],count:{total:0,failed:0,success:0}}},methods:{getList:function(){var t=this;this.isLoading=!0,this.$request.get("/queue",this.$copy(this.searchParams)).then((function(e){e.errCode?t.$message.error(e.message):(t.dataList=e.data.list,t.searchParams.count=e.data.total)})).catch((function(e){t.$message.error(e)})).then((function(){t.isLoading=!1}))},handleSizeChange:function(t){this.searchParams.size=t,this.getList()},handleCurrentChange:function(t){this.searchParams.page=t,this.getList()}}},u=c,d=a("2877"),p=Object(d["a"])(u,s,r,!1,null,"2481238a",null);e["default"]=p.exports},"78ce":function(t,e,a){var s=a("5ca1");s(s.S,"Date",{now:function(){return(new Date).getTime()}})}}]);
//# sourceMappingURL=chunk-7d0e23d1.7e6cfbcf.js.map