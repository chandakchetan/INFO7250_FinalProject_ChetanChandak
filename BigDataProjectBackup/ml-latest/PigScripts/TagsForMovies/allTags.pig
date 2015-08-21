movieData = LOAD '/home/chetanchandak/Documents/ml-latest/movies.csv' using PigStorage(',') as (movieId:int, movieName:chararray, genre:chararray);
tagsData = LOAD '/home/chetanchandak/Documents/ml-latest/tags.csv' using PigStorage(',')  as (userId:int, movieId:int, tags:chararray, timestamp:long);
movieTags = JOIN tagsData by movieId , movieData by movieId;
groupByTitle = GROUP movieTags by movieName;
AllTags = FOREACH groupByTitle GENERATE $0, movieTags.tags;

STORE AllTags into '/home/chetanchandak/Documents/TagsForMovies' using PigStorage();
