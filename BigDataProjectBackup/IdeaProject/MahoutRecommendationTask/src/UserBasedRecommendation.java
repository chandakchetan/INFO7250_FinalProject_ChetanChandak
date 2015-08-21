/**
 * Created by chetanchandak on 8/12/15.
 */
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Scanner;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class UserBasedRecommendation {

    public static void main(String[] args) throws IOException, TasteException {
        DataModel dataModel = new FileDataModel(new File("/home/chetanchandak/Documents/ratingsMahout.csv"));

        UserSimilarity similarity = new PearsonCorrelationSimilarity(dataModel);

        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, dataModel);

        UserBasedRecommender recommender = new GenericUserBasedRecommender(dataModel, neighborhood, similarity);

        PrintWriter writer = new PrintWriter("/home/chetanchandak/Documents/userBasedRecommendationOutput", "UTF-8");

        // Recommending movies for userID
        Scanner input = new Scanner(System.in);
        System.out.println("Enter the userID for which you want to recommend the movies");
        int userID = input.nextInt();

        List recommendationList = recommender.recommend(userID, 10);
        if(!recommendationList.isEmpty()) {
            for (Object recommendation : recommendationList) {
                writer.println(recommendation);
                writer.flush();
            }
        }
        else {
            System.out.println("The UserId does not exists....");
        }

    }
}
