package WuzzufAnalysis;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class main {
    public static void main(String[] args) {
        String path = "src/main/resources/Wuzzuf_Jobs.csv";
        JobDaoSpark J = new JobDaoSpark(path);
        WuzzufMachineLearning wml = new WuzzufMachineLearning(path);
        DataVisualizer DV = new DataVisualizer();
        int limit = 6;

        // Display structure and summary of the data
        wml.SampleSummeryStructure();

        //count the jobs for each company and display in order
        Map<String, Long> EachCompanyJobsNum = J.CountEachCompanyJobs();
        // display popular jobs in pie chart
        DV.PieChart(EachCompanyJobsNum, "Most Demanding Companies", limit);
        System.out.println("Most Demanding Companies :");
        EachCompanyJobsNum = EachCompanyJobsNum.entrySet()
                .stream().limit(limit)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
        for (Map.Entry<String, Long> entry : EachCompanyJobsNum.entrySet()) {
            System.out.println(entry.getKey() + ':' + entry.getValue());
        }
        System.out.println("--------------------------------------------------------------------------------------------\n");

        //what is the most popular job titles
        Map<String, Long> EachJobTitleNum = J.CountJobsNumber();
        // display popular jobs in bar chart
        DV.BarChart(EachJobTitleNum, "Most Popular Jobs", "Job Title", "Job Count", "Jobs Popularity", limit);
//        DV.PieChart(EachJobTitleNum,"Most Popular Job Titles",limit);
        System.out.println("Most Popular Job Titles :");
        EachJobTitleNum = EachJobTitleNum.entrySet()
                .stream().limit(limit)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
        for (Map.Entry<String, Long> entry : EachJobTitleNum.entrySet()) {
            System.out.println(entry.getKey() + ':' + entry.getValue());
        }
        System.out.println("--------------------------------------------------------------------------------------------\n");


        // find out the most popular areas ?
        Map<String, Long> EachLocationNum = J.CountLocationNumber();
        // display popular areas in Bar chart
        DV.BarChart(EachLocationNum, "Most Popular Areas", "Area", "Areas occurrence Count", "Area Popularity", 6);
        System.out.println("Most popular areas :");
        EachLocationNum = EachLocationNum.entrySet()
                .stream().limit(limit)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
        for (Map.Entry<String, Long> entry : EachLocationNum.entrySet()) {
            System.out.println(entry.getKey() + ':' + entry.getValue());
        }
        System.out.println("--------------------------------------------------------------------------------------------\n");

        // find out the most popular Skills ?
        Map<String, Long> EachSkillNum = J.CountSkillssNumber();
        // display popular Skills in Bar chart
        DV.BarChart(EachSkillNum, "Most Needed Skills", "Skills", "Each Skill Count", "Skills Popularity", limit);
        System.out.println("Most Important Skills required :");
        EachSkillNum = EachSkillNum.entrySet()
                .stream().limit(limit)
                .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
        for (Map.Entry<String, Long> entry : EachSkillNum.entrySet()) {
            System.out.println(entry.getKey() + ':' + entry.getValue());
        }
        System.out.println("--------------------------------------------------------------------------------------------\n");




    }
}
