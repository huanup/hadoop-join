## hadoop-join
#### 基础数据
###### dept部门信息
    部门编号 部门名称 城市
###### emp 员工信息
    员工号 姓名 角色 下属 时间 薪资 无视 部门
+  reduce join -> EmpMoreThanLeader 统计员工工资高于领导
   
+  mapper join ->SumDeptSalary 统计部门工资

+  global sort ->EmpSalarySort 员工工资排序 

+  topN -> Top3Salary 统计员工工资前三
   
