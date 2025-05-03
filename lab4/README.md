### JAR包运行方法：

#### 任务1：`<INPUT> <OUTPUT>`

本地

```bash
$ hadoop jar job1/target/job1-4.jar com.mapred.MutualFollow ./dataset ./job1-output
```

集群

```bash
$ yarn jar job1-4.jar com.mapred.MutualFollow /user/root/Exp4 ./lab4-1-out
```

#### 任务2: `<THRESHOLD> <INPUT> <OUTPUT> <MUTUALS>`
> 从左到右，分别是设定的划分阈值、输入数据集、输出路径、（任务一得到的）互关列表

本地

```bash
$ hadoop jar job2/target/job2-5.jar com.mapred.SharedFollow 10 ./dataset ./job2-output ./mutuals.txt
```

集群

```bash
$ yarn jar job2-5.jar com.mapred.SharedFollow 10 /user/root/Exp4 ./lab4-2-out ./lab4-1-out/part-r-00000
```

#### 任务3: `<INPUT> <OUTPUT> <MUTUALS>`
> 从左到右，分别是: 输入数据集、输出路径、（任务一得到的）互关列表

本地

```bash
$ hadoop jar job3/target/job3-2.jar com.mapred.FriendReco ./dataset ./job3-output mutuals.txt
```

集群

```bash
$ yarn jar job3-2.jar com.mapred.FriendReco /user/root/Exp4 ./lab4-3-out ./lab4-1-out/part-r-00000
```