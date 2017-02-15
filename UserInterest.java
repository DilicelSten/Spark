import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;  

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

/**
 * 
 * @author totoro
 * target:预测用户的兴趣
 * begin_time:2017.2.14
 * finish_time:2017.2.
 *
 */

public class UserInterest {

	static NaiveBayesModel model;

	public static void main(String[] args) throws Exception{
		String  testStr =null;
		//进行Spark的配置
		SparkConf conf = new SparkConf().setAppName("GMM").setMaster("local");
		conf.set("spark.testing.memory","2147480000");    //Spark的运行配置，意指占用内存2G
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		//读取txt文件并利用hanlp切词形成一个list，作为测试样本
		String filepath = "/home/hadoop/result/19岁金鱼想当歌手.txt";
		File inputfile = new File(filepath);
		rewriteFile(filepath);
		String content = readTxtFile(inputfile); //获取txt的内容
		System.out.print(content);
		Segment segment = HanLP.newSegment();
		List<Term> termList = segment.seg(content); //用Hanlp进行切词形成List
		for(Term term : termList){
            testStr += term.word + " ";  //使用term.word去掉属性部分
        }
		System.out.print(testStr);
		
		//获取标签特征词(！！发现隐藏文件，由于使用过gedit打开过)，形成一个vocabulary
		List<String > vocabulary = new ArrayList<String>();
		File dir = new File("/home/hadoop/项目内容/类别库");
		File[] files = dir.listFiles();   //获取不同类别的标签文件
		System.out.println(files.length);
		StringBuilder sb = new StringBuilder();
		for(File file : files){
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			while((line =  br.readLine()) != null){
				sb.append(line);  //按"`"分割不同类别的标签
			}
			sb.append(line + "`");
		}
		String[] tags = sb.toString().trim().split("`");
		List<String> newTags = new ArrayList<String>();
		for(String tag: tags){
			if(tag.length() > 4){
				newTags.add(tag); //去除空行标签
			}
		}
		Object[] newtags = newTags.toArray();
		List<Tuple2<Integer,String>> list = new ArrayList<Tuple2<Integer,String>>();  //记录每类中的标签
		for(int i = 0; i<newtags.length;i++){
			Tuple2<Integer,String> classWithTags = new Tuple2<Integer, String>(i,(String)newtags[i]);
			System.out.println(classWithTags);
			list.add(classWithTags);
			String[] tokens = ((String)newtags[i]).split("/");
			for(String tag: tokens){
				vocabulary.add(tag);
			}
		}
		
		//获取训练样本
		JavaPairRDD<Integer,String> trainRDD = sc.parallelizePairs(list);   //将每类的标签词转化为RDD
		JavaPairRDD<Integer,String> trainSetRDD = trainRDD.mapValues(new ToTrainSet(vocabulary));  //将标签词转化为向量模型
		List<Tuple2<Integer,String>> trainSet = trainSetRDD.collect();
		writeTrainSet(trainSet);  //写成libsvm文件格式，以方便训练
		System.out.println("trainset is ok");
		
		//读取训练集并训练模型
		String path = "./trainset";
		JavaRDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(sc.sc(),path).toJavaRDD();
		model = NaiveBayes.train(trainData.rdd(),1.0);
		System.out.println("model is ok");

		//预测测试集
		double[] testArray = sentenceToArrays(vocabulary,testStr);
		writeTestSet(testArray);
		String testPath = "./testset";
		JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(sc.sc(), testPath).toJavaRDD();

		//多元分类预测
		JavaRDD<double[]> resultData = testData.map(new GetProbabilities());
		List<double[]> result = resultData.collect();   //保存的是每个测试样本所属于不同类别的概率值
		Map<Integer,Double> map = new HashMap<Integer,Double>(); 
		for(double[] one: result){
			for(int i=0;i<one.length;i++){
//				System.out.println("class"+ i + ":" + one[i]);
				map.put(i, one[i]);
			}
		}
		List<Map.Entry<Integer,Double>> maplist = new ArrayList<Map.Entry<Integer,Double>>(map.entrySet()); // 转化为list，然后对值排序
		Collections.sort(maplist,new Comparator<Entry<Integer,Double>>() {  
            public int compare(Entry<Integer,Double> o1,  Entry<Integer,Double> o2) {  
                if(o1.getValue()> o2.getValue()){  
                    return -1;  
                }
                else if(o1.getValue()< o2.getValue()){  
                    return 1;
                }
				return 0;
            }
		}); 
		   printEntry(maplist); 
           System.out.println("报告短短，测试完毕！");
		}
	
	public static void printEntry(List<Entry<Integer,Double>> mapList){  
        //Set<Map.Entry<Student,Teacher>> s = map.entrySet();  
        for(Entry<Integer,Double> entry : mapList){  
            System.out.println(entry.getKey()+":"+entry.getValue());  
        }  
    }  
	
	//将训练样本写成libsvm格式，形成trainset文件
	public static void writeTrainSet(List<Tuple2<Integer,String>> list) throws Exception{
		File file = new File("./trainset");
		PrintWriter pr = new PrintWriter(new FileWriter(file));
		for(Tuple2<Integer,String> one : list){   //将每个训练样本以libsvm格式保存到trainset文件当中
			String label = String.valueOf(one._1); //训练样本的类别属性
			String vector = one._2(); //训练样本的向量模型
			String[] indexes = vector.split(" ");
			pr.print(label + " ");
			String value = "";
			for(int i = 0; i<indexes.length;i++){
				value += (i+1) + ":" + indexes[i] + " ";  //i+1是因为libsvm文件的index是从1开始
			}
			pr.print(value.trim());
			pr.println();
		}
		pr.close();
	}
	
	//将测试样本写成libsvm格式，形成testset文件(与writeTrainSet一样)
	public static void writeTestSet(double[] testArray) throws Exception{
		File file = new File("./testset");
		PrintWriter pr = new PrintWriter(new FileWriter(file));
		pr.print("0"+" ");
		String value = "";
		for(int i=0; i<testArray.length; i++){
			value += (i+1) + ":" + testArray[i] + " ";
		}
		pr.print(value.trim());
		pr.close();
	}

	//将输入的测试样本转化成数组形式
	public static double[] sentenceToArrays(List<String> vocabulary, String sentence){
		 double[] vector = new double[vocabulary.size()];
		 for(int i=0; i<vocabulary.size();i++){
			 vector[i] = 0;
		 }
		 String[] tags = sentence.split(" ");
		 for(String tag: tags){
			 if(vocabulary.contains(tag)){
				 int index = vocabulary.indexOf(tag);
				 System.out.println(index);
				 vector[index] += 1;
			 }
		 }
		 return vector;
}
	
	//读取txt文件
	public static String readTxtFile(File file){
        StringBuilder result = new StringBuilder();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                result.append(System.lineSeparator()+s);
            }
            br.close();    
        }catch(Exception e){
            e.printStackTrace();
        }
        return result.toString();
    }
	
	//重写文件
	public static void rewriteFile(String path) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(new File (path)));
		String content = "";
		String line;
		while((line = br.readLine()) != null){
			content += line.replaceAll("\\s*", "");;
		}
		br.close();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
		bw.append(content);
		bw.close();
	}
	
	static class CreateLabel implements Function<Tuple2<Integer,Vector>, LabeledPoint>{

		public LabeledPoint call(Tuple2<Integer, Vector> one) throws Exception {
			double label = (double)one._1();
			Vector vector = one._2();
			return new LabeledPoint(label, vector);
		}
		
	}
	
	static class FlatTags implements  Function<String, Iterable<String>>{

		public Iterable<String> call(String tags) throws Exception {
			// TODO Auto-generated method stub
			return Arrays.asList(tags.split("/"));
		}
	}
	
static class ToVector implements Function<String, Vector>{
		
		List<String> vocabulary = null;
		public ToVector(List<String> vocabulary){
			this.vocabulary = vocabulary;
		}
		
		public Vector call(String tag) throws Exception {
			// TODO Auto-generated method stub
			int index = vocabulary.indexOf(tag);
			double[] arrays = new double[vocabulary.size()];
			for(int i = 0; i<arrays.length;i++){
				if( i == index){
					arrays[i] = 1;
				}
				else{
					arrays[i] = 0;
				}
			}
			return Vectors.dense(arrays);
		}
	}

	//对训练集进行处理
	static class ToTrainSet implements Function<String,String>{
		List<String> vocabulary = null; //标签特征库
		public ToTrainSet(List<String> vocabulary){
			this.vocabulary = vocabulary;
		}
		public String call(String sentence) throws Exception{
			int length = vocabulary.size();  //特征维度
			String[] tags = sentence.split("/");
			List<Integer> tagsindex = new ArrayList<Integer>();
			for(int i = 0;i<tags.length;i++){
				tagsindex.add(vocabulary.indexOf(tags[i]));
			}
			String vector = ""; //将特征向量转变为String类，节省空间
			for(int i = 0;i < length;i++){
				if(tagsindex.contains(i)){
					vector += String.valueOf(1) + " ";
				}
				else{
					vector += String.valueOf(0) + " ";
				}
			}
			return vector.trim();
		}
	}

	static class GetProbabilities implements Function<LabeledPoint, double[]>{
		public double[] call(LabeledPoint p){
			Vector predict = model.predictProbabilities(p.features());
			double[] probabilities = predict.toArray();
			return probabilities;
		}
	}
	static class Predict implements Function<LabeledPoint, Double> {
		
		public Double call(LabeledPoint p){
			double predict = model.predict(p.features());
			return predict;
		}
	}
}
