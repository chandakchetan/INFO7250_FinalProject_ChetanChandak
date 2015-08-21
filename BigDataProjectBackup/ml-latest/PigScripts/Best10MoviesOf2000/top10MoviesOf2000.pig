movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
decade2000 = filter movieRatings by $4>='2001' and $4<='2010';
groupByTitle =  GROUP decade2000 by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(decade2000.ratings) as averageMovieRating;
orderByAverageRatingDesc = ORDER averageRatingOfEachMovie by averageMovieRating DESC;
best10MoviesOf2000 = LIMIT orderByAverageRatingDesc 10;
STORE best10MoviesOf2000 into '/home/chetanchandak/Documents/Best10MoviesOf2000' using PigStorage();
