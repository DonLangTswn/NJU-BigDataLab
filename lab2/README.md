### JAR包运行方法：

#### 任务1：`<INPUT> <OUTPUT>`

本地

```bash
$ hadoop jar job1-3.jar com.mapred.InvertedIndex ./HarryPoter ./HarryPotter-out
```

集群

```bash
$ yarn jar job1-7.jar com.mapred.InvertedIndex /user/root/Exp2 ./job1-out
```

#### 任务2: `<INPUT> <OUTPUT>`

本地

```bash
$ hadoop jar job2-2.jar com.mapred.TFIDF ./HarryPoter ./HarryPotter-TFIDF-out
```

集群

```bash
$ yarn jar job2-3.jar com.mapred.TFIDF /user/root/Exp2 ./job2-out
```

#### 任务3: `<INPUT> <OUTPUT> <STOPWORDS>`

本地

```bash
$ hadoop jar job3-2.jar com.mapred.InvertedIndex ./HarryPoter ./HarryPotter-out cn_stopwords.txt
```

集群

```bash
$ yarn jar job3-5.jar com.mapred.InvertedIndex /user/root/Exp2 ./job3-out /user/root/Exp2/cn_stopwords.txt
```