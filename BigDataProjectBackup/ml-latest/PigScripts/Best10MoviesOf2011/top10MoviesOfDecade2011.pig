movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
decade2011 = filter movieRatings by $4>='2011' and $4<='2015';
groupByTitle =  GROUP decade2011 by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(decade2011.ratings) as averageMovieRating;
orderByAverageRatingDesc = ORDER averageRatingOfEachMovie by averageMovieRating DESC;
best10MoviesOf2011 = LIMIT orderByAverageRatingDesc 10;
STORE best10MoviesOf2011 into '/home/chetanchandak/Documents/Best10MoviesOf2011' using PigStorage();
