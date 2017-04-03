import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

                                      
public class Algorithm_vverify {
	
	public static void main(String[] args) throws IOException{
		Map<String,String> human = new HashMap<String,String>();
		Map<String,String> predict = new HashMap<String,String>();
		String humanpath = "/home/hadoop/Seeing项目内容/result/人工标注/人工标注用户";
		String predictpath = "/home/hadoop/Seeing项目内容/result/人工标注/新机器";
		//将人工标注的放于human中
		BufferedReader human_br = new BufferedReader(new FileReader(new File(humanpath)));
		String human_interest = "";
		while((human_interest = human_br.readLine()) != null){
			String[] content = human_interest.split("	");
			human.put(content[0], content[1]);		
			}
		//将机器标注的放于predict中
		BufferedReader predict_br = new BufferedReader(new FileReader(new File(predictpath)));
		String predict_interest = "";
		while((predict_interest = predict_br.readLine()) != null){
			String[] content1 = predict_interest.split("	");
			predict.put(content1[0], content1[1]);
		}
		//计算准确率和召回率
		//先转成set
		Set human_set = human.entrySet();
		Set predict_set = predict.entrySet();
		int i  = 0;
		double accuracy = 0;
		double recall = 0;
		for(Iterator iter1 = predict_set.iterator(); iter1.hasNext();){
			String[] predictinterests = {};
			String[] humaninterests = {};
			//交集个数
			double  result_insect =0;
			Map.Entry<String , String> entry1 = (Map.Entry<String, String>) iter1.next();
			String predictkey = entry1.getKey();
			String predictvalue = entry1.getValue();
			predictinterests = predictvalue.split(",");
			for(Iterator iter2 = human_set.iterator();iter2.hasNext();){
				Map.Entry<String, String> entry2 = (Map.Entry<String, String>) iter2.next();
				String humankey = entry2.getKey();
				String humanvalue = entry2.getValue();
//				System.out.println(humanvalue);
				humaninterests = humanvalue.split(",");
				if(humankey.equals(predictkey)){
					//求交集
					i++;
					result_insect = intersect(predictinterests, humaninterests);
//					System.out.println(result_insect);
					double a = predictinterests.length;
					double b = humaninterests.length;
					accuracy = (result_insect/predictinterests.length + accuracy*(i-1))/i;
					recall =( result_insect/humaninterests.length + recall *(i-1))/i;
//					System.out.println(i+ " "+ result_insect + " "+ a + " "+ b);
				}
			}
			if( i == 50){
				System.out.println("个数为50的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 100){
				System.out.println("个数为100的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 150){
				System.out.println("个数为150的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 200){
				System.out.println("个数为200的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 250){
				System.out.println("个数为250的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 300){
				System.out.println("个数为300的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 350){
				System.out.println("个数为350的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 400){
				System.out.println("个数为400的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 450){
				System.out.println("个数为450的准确率：" + accuracy +" ;召回率为：" + recall);
			}
			if( i == 499){
				System.out.println("个数为500的准确率：" + accuracy +" ;召回率为：" + recall);
			}
		}
	}
	//求两个String类型数组的交集
	public static int  intersect(String[] arr1,String[] arr2){
		Map<String,Boolean> map = new HashMap<String,Boolean>();
		LinkedList<String> list  = new LinkedList<String>();
		for(String str : arr1){
			if(!map.containsKey(str)){
				map.put(str, Boolean.FALSE);
			}
		}
		for(String str:arr2){
			if(map.containsKey(str)){
				map.put(str, Boolean.TRUE);
			}
		}
		for(Entry<String,Boolean> e : map.entrySet()){
			if(e.getValue().equals(Boolean.TRUE)){
				list.add(e.getKey());
			}
		}
		return list.size();
	}
}
