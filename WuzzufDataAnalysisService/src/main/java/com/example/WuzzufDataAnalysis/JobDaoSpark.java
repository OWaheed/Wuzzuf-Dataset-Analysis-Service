package com.example.WuzzufDataAnalysis;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

@RestController

public class JobDaoSpark {

    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    SparkConf configuration= new SparkConf().setAppName("WuzzufDataAnalysis").setMaster("local[3]");
    JavaSparkContext sparkContext=new JavaSparkContext(configuration);
    JavaRDD<String> wuzzuf_dataset_strings= sparkContext.textFile(path).distinct();
    DataVisualizer d = new DataVisualizer();


    @GetMapping(path = "CountEachCompanyJobs")
    public Map<String, Long> CountEachCompanyJobs() {
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.ERROR);

        Map<String, Long> Companies = new LinkedHashMap<>();
        wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingCompany)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(company -> Companies.put(company.getKey(), company.getValue()));
        return Companies;

    }
    @GetMapping(path = "CountJobsNumber")
    public Map<String, Long> CountJobsNumber() {
        Map<String, Long> Jobs = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingTitle)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(Job -> Jobs.put(Job.getKey(), Job.getValue()));
//        WuzzufMachineLearning.SampleSummeryStructure();
        return Jobs;

    }
    @GetMapping(path = "CountLocationNumber")
    public Map<String, Long> CountLocationNumber() {
        Map<String, Long> Locations = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingLocation)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(location -> Locations.put(location.getKey(), location.getValue()));
        return Locations;

    }
    @GetMapping(path = "CountSkillsNumber")
    public Map<String, Long> CountSkillsNumber() {
        Map<String, Long>  Skills = new LinkedHashMap<>();
        JavaRDD<String> EachSkill = this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingSkills)
                .filter (StringUtils::isNotBlank)
                .flatMap(skill->Arrays.asList (
                        skill .split (",")).iterator ());
        EachSkill.countByValue().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(skill -> Skills.put(skill.getKey(), skill.getValue()));
        return Skills;
    }

    public static String preparingTitle(String dataLine) {
        try {
            String Title = dataLine.split(",")[0];
            return Title;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String preparingCompany(String dataLine) {
        try {
            String Company = dataLine.split(",")[1];
            return Company;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String preparingLocation(String dataLine) {
        try {
            String Location = dataLine.split(",")[2];
            return Location;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String preparingType(String dataLine) {
        try {
            String Type = dataLine.split(",")[3];
            return Type;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }

    }

    public static String preparingLevel(String dataLine) {
        try {
            String Level = dataLine.split(",")[4];
            return Level;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String preparingYearsExp(String dataLine) {
        try {
            String YearsExp = dataLine.split(",")[5];
            return YearsExp;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }

    public static String preparingSkills(String dataLine) {
        try{
            return dataLine.split(",\"")[1].replace("\"","");
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    @RequestMapping(path = "")
    public String Menu(){
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"<style>\n" +
                "table {\n" +
                "  font-family: arial, sans-serif;\n" +
                "  border-collapse: collapse;\n" +
                "  width: 100%;\n" +
                "}\n" +
                "\n" +
                "td, th {\n" +
                "  border: 1px solid #dddddd;\n" +
                "  text-align: left;\n" +
                "  padding: 8px;\n" +
                "}\n" +
                "\n" +
                "tr:nth-child(even) {\n" +
                "  background-color: #dddddd;\n" +
                "}\n" +
                "</style>\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>Wuzzuf Dataset Services</h2>\n" +
                "\n" +
                "<table>\n" +
                "  <tr>\n" +
                "    <th>Service</th>\n" +
                "    <th>Link</th>\n" +
                "    \n" +
                "  </tr>\n" +

                "  <tr>\n" +
                "    <td>Display Sample of the  dataframe </td>\n" +
                "    <td><a href=\"http://localhost:8080/display\">Display Sample of DataSet </a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Display Structure  of the  dataframe </td>\n" +
                "    <td><a href=\"http://localhost:8080/structure\">Display Structure of DataSet</a></td>\n" +
                "    \n" +
                "  </tr>\n" +

                "  <tr>\n" +
                "    <td>Count the jobs for each coutry and display  that  in  order</td>\n" +
                "    <td><a href=\"http://localhost:8080/CountEachCompanyJobs\">Each Company offers Number</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Show Companny Pie Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/CompaniesPie\">pie chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +

                "  <tr>\n" +
                "    <td>Find out what is the most popular job  titles?</td>\n" +
                "    <td><a href=\"http://localhost:8080/CountJobsNumber\">Number Of Offers For Each Title</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Showw Number Of Offers For Each Title pie chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/TitlesBarChart\">Bar chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Find out the  most popular areas</td>\n" +
                "    <td><a href=\"http://localhost:8080/CountLocationNumber\">Number Of Offers For Each Area</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Show  popular Areas Bar Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/AreasBarChart\">Bar chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>print Skills one  by one ordering by most needed  skills first</td>\n" +
                "    <td><a href=\"http://localhost:8080/CountSkillsNumber\">Most Important Skills</a></td>\n" +
                "  </tr> \n" +
                "  <tr>\n" +
                "    <td>Most Important Skills Bar Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/SkillsBarChart\">Bar chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "</table>\n" +
                "\n" +
                "</body>\n" +
                "</html>");

        return m.toString();
    }
    @RequestMapping(path = "CompaniesPie")
    public String JobsPieChart(){
        d.PieChart(CountEachCompanyJobs(), "Most Demanding Companies", 6,"chart1");
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most demanding Companies  For jobs?</h2>\n" +
                "<img src=\"chart1.png\">"+
                "</body>\n" +
                "</html>");

        return m.toString();


    }
    @RequestMapping(path = "SkillsBarChart")
    public String SkillsBarChart(){
        d.BarChart(CountSkillsNumber(), "Most Needed Skills", "Skills", "Each Skill Count", "Skills Popularity", 6,"chart4");
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most Needed Skills </h2>\n" +
                "<img src=\"chart4.png\">"+
                "</body>\n" +
                "</html>");

        return m.toString();


    }
    @RequestMapping(path = "AreasBarChart")
    public String AreasBarChart(){
        d.BarChart(CountLocationNumber(), "Most Popular Areas", "Area", "Areas occurrence Count", "Area Popularity", 6,"chart3");
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the most popular areas ?: </h2>\n" +
                "<img src=\"chart3.png\">"+
                "</body>\n" +
                "</html>");

        return m.toString();
    }
    @RequestMapping(path = "TitlesBarChart")
    public String TitlesBarChart(){
        d.BarChart(CountJobsNumber(), "Most Popular Jobs", "Job Title", "Job Count", "Jobs Popularity", 6,"chart2");
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most Popular Job Titles </h2>\n" +
                "<img src=\"chart2.png\" >"+
                "</body>\n" +
                "</html>");

        return m.toString();
    }

    List<HashMap> dfToJson(Table t, int limit){
        List<HashMap> jsonList = new ArrayList();
        List<String> keys = t.columnNames();
        int size = t.columnCount();
        Iterator<Row> rows = t.stream().iterator();
        if (limit != 0){
            rows = t.stream().limit(5).iterator();
        }

        while (rows.hasNext()){
            Row values = rows.next();
            HashMap row = new HashMap<>();
            for (int i=0; i<size; i++){
                row.put(keys.get(i), values.getObject(i));
            }
            jsonList.add(row);

        }
        return jsonList;
    }

    @RequestMapping("/display")
    public HashMap<String, List<HashMap>> display(){

        try {
            Table t= Table.read().csv(path);//.dropDuplicateRows().dropRowsWithMissingValues();
            HashMap<String, List<HashMap>> result = new HashMap();
            result.put("jobs", dfToJson(t,5));
            return result;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
    @RequestMapping("/structure")
    public HashMap<String, List<HashMap>> description(){


        try {
            Table t= Table.read().csv(path);
            Table struct = t.structure();
            HashMap<String, List<HashMap>> result = new HashMap();
            result.put("structure", dfToJson(struct,0));
            return result;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }



}
