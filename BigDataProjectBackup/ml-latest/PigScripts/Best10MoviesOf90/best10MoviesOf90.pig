movieRatings = LOAD '/home/chetanchandak/Documents/ml-latest/JoinedTables.csv' using PigStorage(',') as (userId:int, movieId:int, ratings:float, movieName:chararray, year:chararray, genre:chararray);
decade90 = filter movieRatings by $4>='1991' and $4<='2000';
groupByTitle =  GROUP decade90 by movieName;
averageRatingOfEachMovie = FOREACH groupByTitle GENERATE $0, AVG(decade90.ratings) as averageMovieRating;
orderByAverageRatingDesc = ORDER averageRatingOfEachMovie by averageMovieRating DESC;
best10MoviesOf90 = LIMIT orderByAverageRatingDesc 10;
STORE best10MoviesOf90 into '/home/chetanchandak/Documents/Best10MoviesOf90' using PigStorage();
