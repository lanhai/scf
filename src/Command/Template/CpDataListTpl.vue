<template>
  <div>
    <el-form :inline="true" :model="searchParams" ref="searchForm" label-width="90px" class="demo-form-inline">
      <el-form-item prop="keyword" class="ml-10">
        <el-input size="small" v-model="searchParams.keyword" placeholder="关键字"></el-input>
      </el-form-item>
      <el-form-item>
        <el-button icon="el-icon-search" type="primary" @click="search()" size="small" :loading="isLoading">查询
        </el-button>
        <el-button icon="el-icon-plus" type="primary" size="small" @click="add()">添加</el-button>
      </el-form-item>
    </el-form>
    <el-table :data="dataList" stripe style="width: 100%" row-key="id">
      <el-table-column prop="id" label="ID">
      </el-table-column>
      <el-table-column label="操作">
        <template
            v-slot="scope">
          <el-button type="text" class="c-blue" @click="edit(scope.$index)" icon="el-icon-edit-outline">编辑</el-button>
          <el-button type="text" class="c-red" @click="remove(scope.$index)" icon="el-icon-delete">删除</el-button>
        </template>
      </el-table-column>

    </el-table>
    <!--翻页-->
    <el-pagination
        background
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :current-page.sync="searchParams.page"
        :page-sizes="[1, 5, 10, 15, 20, 50, 100]"
        :page-size="searchParams.size"
        layout="total,sizes, prev, pager, next"
        :total="searchParams.count">
    </el-pagination>
    <el-dialog :title="mode==='add'?'添加数据':'编辑数据'" :visible.sync="dialogVisible" :close-on-click-modal="false" width="60%">
      <el-form :model="input" :rules="rules" status-icon ref="form" label-width="120px" class="follow-form">
        <el-form-item label="字段名称：" prop="name">
          <el-col :span="5">
            <el-input type="input" placeholder="填写字段名称" v-model="input.name"></el-input>
          </el-col>
        </el-form-item>
        <el-form-item label="单选：" prop="radio">
          <el-col :span="15">
            <el-radio-group v-model="input.radio">
              <el-radio label="1">选项一</el-radio>
              <el-radio label="2">选项二</el-radio>
            </el-radio-group>
          </el-col>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">取 消</el-button>
        <el-button type="primary" @click="submit()" :loading="isLoading">保 存</el-button>
      </div>
    </el-dialog>
  </div>

</template>

<script>

export default {
  name: "DataListTpl",
  components: {},

  //默认执行的方法
  mounted() {
    this.getList()
  },
  // 数据对象
  data() {
    return {
      dialogVisible: false,
      isLoading: false,
      searchParams: {
        keyword: this.$route.query.keyword || '',
        export: '',
        count: 0,
        size: Number(this.$route.query.size) || 10,
        page: Number(this.$route.query.page) || 1,
      },
      dataList: [],
      input: {},
      rules: {
        name: {
          required: true,
          message: '请填写名称',
          trigger: 'submit'
        },
      },
      mode: 'add'
    }
  },
  methods: {
    add() {
      this.dialogVisible = true;
      this.input = {};
    },
    edit(index) {
      this.input = this.dataList[index]
      this.dialogVisible = true;
    },
    //提交
    submit() {
      this.$refs['form'].validate((valid) => {
        this.isLoading = true;
        if (valid) {
          this.$request.post('{saveUrl}', this.$copy(this.input)).then(result => {
            if (result.errCode) {
              this.$message.alert(result.message)
            } else {
              this.dialogVisible = false
              this.$message.success('保存成功')
              this.getList()
            }
          }).catch(error => {
            this.$message.error(error)
          }).finally(() => {
            this.getList()
            this.isLoading = false
            this.dialogVisible = false
          })
        } else {
          this.isLoading = false
          return false;
        }
      });
    },
    //删除
    remove(index) {
      let item = this.dataList[index]
      this.$message.confirm({message: '确定要删除吗?'}, () => {
        this.$loading.show('删除中')
        this.$request.post('{removeUrl}', {id: item.id}).then(result => {
          if (result.errCode) {
            this.$message.alert(result.message)
          } else {
            this.$message.success('删除成功')
            this.getList()
          }
        }).catch(error => {
          this.$message.error(error)
        }).finally(() => {
          this.$loading.close()
        })
      })
    },
    //获取列表
    getList() {
      this.isLoading = true;
      let searchParams = this.$copy(this.searchParams)
      this.$request.get('{listUrl}', searchParams).then(result => {
        if (result.errCode) {
          this.$message.error(result.message);
        } else {
          this.dataList = result.data.list;
          this.searchParams.count = result.data.total;//总条数
        }
      }).catch(error => {
        this.$message.error(error);
      }).then(() => {
        this.isLoading = false;
      });
    },
    //搜索
    search() {
      this.$router.push({
        path: this.$route.path,
        query: this.searchParams
      })
      this.dataList = []
      this.getList()
    },
    //设置单页显示梳理
    handleSizeChange(val) {
      this.searchParams.page = 1;
      this.searchParams.size = val;
      this.search();
    },
    //分页
    handleCurrentChange(val) {
      this.searchParams.page = val;
      this.search();
    },
  }
}
</script>