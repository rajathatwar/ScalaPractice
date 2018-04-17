package uis.bigdataClass.RecommenderSystem;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class EvaluateRecommender {

	public static void main(String[] args) throws Exception {
		DataModel model = new FileDataModel(new File(args[0]));
		RecommenderEvaluator MAEevaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
		RecommenderEvaluator RMSEevaluator = new RMSRecommenderEvaluator();
		RecommenderBuilder builder = new MyRecommenderBuilder();
		double result = MAEevaluator.evaluate(builder, null, model, 0.9, 1.0);
		System.out.println("Average absolute Difference: " + result);
		result = RMSEevaluator.evaluate(builder, null, model, 0.9, 1.0);
		System.out.println("Root Mean Squared Error: " + result);
		RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = evaluator.evaluate(builder, null, model, null, 10,
				GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		System.out.println("precision: " + stats.getPrecision());
		System.out.println("Recall: " + stats.getRecall());
	}
}

class MyRecommenderBuilder implements RecommenderBuilder {

	public Recommender buildRecommender(DataModel model) throws TasteException {
		
	/*	
	 * I am choosing iteamBase as data is small I was aiming to get less "Average absolute Difference"
	 *  and "Root Mean Squared Error" in iteamBase this entities were less as compared to UserBase.
	 */
		
		ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);
		// ItemSimilarity similarity = new LogLikelihoodSimilarity(model);
		// ItemSimilarity similarity = new EuclideanDistanceSimilarity(model);

		// UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		// UserSimilarity similarity = new LogLikelihoodSimilarity(model);
		// UserSimilarity similarity = new EuclideanDistanceSimilarity(model);

		// UserNeighborhood Usr_Nbr = new NearestNUserNeighborhood(90,
		// similarity, model);
		// UserNeighborhood Usr_Nbr = new NearestNUserNeighborhood(90, 0.8,
		// similarity, model,0.7);
		// UserNeighborhood Usr_Nbr = new NearestNUserNeighborhood(90, 0.8,
		// similarity, model);
		return new GenericItemBasedRecommender(model, similarity);
		// return new GenericUserBasedRecommender(model, Usr_Nbr, similarity);
	}

}
