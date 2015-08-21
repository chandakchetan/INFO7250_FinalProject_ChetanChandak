
/**
 * Created by chetanchandak on 8/13/15.
 */

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

public class ItemBasedRecommendation {

    public static void main(String[] args) throws IOException, TasteException {
        DataModel dataModel = new FileDataModel(new File("/home/chetanchandak/Documents/ratingsMahout.csv"));

        LogLikelihoodSimilarity logLikelihoodSimilarity = new LogLikelihoodSimilarity(dataModel);

        GenericItemBasedRecommender genericItemBasedRecommender = new GenericItemBasedRecommender(dataModel, logLikelihoodSimilarity);

        PrintWriter writer = new PrintWriter("/home/chetanchandak/Documents/movieBasedRecommendationOutput", "UTF-8");

        for( LongPrimitiveIterator longPrimitiveIteratorForMovies = dataModel.getItemIDs(); longPrimitiveIteratorForMovies.hasNext();){
            Long movieId = longPrimitiveIteratorForMovies.nextLong();
            List<RecommendedItem> recommendations = genericItemBasedRecommender.mostSimilarItems(movieId, 3);

            for (RecommendedItem recommendation : recommendations) {
                writer.println(movieId + ", " + recommendation.getItemID() + ", " + recommendation.getValue());
                writer.flush();
            }
        }
        writer.close();
    }

}
