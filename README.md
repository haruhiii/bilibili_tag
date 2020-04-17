# bilibili_tag
  B站视频标签爬虫，用于估算某一用户群体占比

  ## 运行过程截图
  
  ![image](https://github.com/vrilc/bilibili_tag/blob/master/imgae.jpg)
  ![image2](https://github.com/vrilc/bilibili_tag/blob/master/image2.jpg)
  
# 功能
  * 从1~4_0000_0000选取随机数作为UID
  * 判断是否活跃用户（有发布或公开收藏的视频）
  * 判断所有公开或收藏的视频是否含有特定标签,记录为目标用户,存入数据库
  * 重复以上过程直到目标用户或活跃用户达到退出条件
  * 将数据库内容导出为XLS文件
# 使用
## 下载
```
git clone https://github.com/vrilc/bilibili_tag.git
```
或 [直接下载](https://github.com/vrilc/bilibili_tag/archive/master.zip)

## 运行
```
python clear.py
```

# 结果
* 结果会保存为当前目录下的SQLite文件,文件名为cache.db。
* 同时会保存一份cache.xls以便使用Excel打开
  
## 坑
* IP代理没写
* 对于单个用户,只获取其最新的1000个发布的视频和每个收藏夹的前400个视频
* 协程超过20基本上就被封IP,几分钟到几小时不等
  Whatever :(
# License
MIT
