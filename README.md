# BigDataRampUp.SparkSql
1st Homework - Facebook events attendance

Input data:
- parquet file *(cityid: Int, date:Int, tagids:Array[Int])* - unique tag ids by city and date
- *city.us.txt* - city dictionary
- *user.profile.tags.us.txt* - user tags dictionary

Output: parquet file *(cityid:Int, date:Int, total_attendance:Int, attendance:Array[(String,Int)])* - attendance by city, date and keyword

App is configured via configuration file passed as the first (and the least) command line parameter.
See [configuration example](cfg/app.conf.sample)
