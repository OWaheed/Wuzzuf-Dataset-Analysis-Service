package WuzzufAnalysis;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JobDaoSpark {
    JavaRDD<String> wuzzuf_dataset_strings;
    SparkConf configuration;
    JavaSparkContext sparkContext;
    String path;
    Map<String, Long> Jobs; // to store num of each job
    Map<String, Long> Companies;// to store num of offers for each country
    Map<String, Long> Locations;// to store  number of each location
    Map<String, Long> Skills;//  to store number of  each skill
    ArrayList<String> AllSkills;

    public JobDaoSpark(String path) {
//        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.ERROR);
        this.path = path;
        // preparing configuration and  allowing spark to take 3 of the cores to run the code with it
        this.configuration = new SparkConf().setAppName("WuzzufDataAnalysis").setMaster("local[3]");

        // preparing the Spark Context
        this.sparkContext = new JavaSparkContext(this.configuration);

        //  Reading Data From csv into Spark Rdd and removing duplicates
        this.wuzzuf_dataset_strings = this.sparkContext.textFile(this.path).distinct();

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

        try {

            String Skills = dataLine.split(",")[7];

            return Skills;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }
    }
    public static String preparingSkillss(String dataLine) {
        try{
        return dataLine.split(",\"")[1].replace("\"","");
        } catch (ArrayIndexOutOfBoundsException e) {
        return "";
    }
    }
//    public  void preparingSkills(String dataLine) {
//            ArrayList<String> Skills = (ArrayList<String>) Arrays.asList(dataLine.split(",\"")[1].
//                                       replace("\"","").split(","));
//        this.AllSkills.addAll(Skills);
//
//
//    }

    public Map<String, Long> CountEachCompanyJobs() {
        this.Companies = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingCompany)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(company -> Companies.put(company.getKey(), company.getValue()));
        return this.Companies;

    }

    public Map<String, Long> CountJobsNumber() {
        this.Jobs = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingTitle)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(Job -> Jobs.put(Job.getKey(), Job.getValue()));
        return this.Jobs;

    }

    public Map<String, Long> CountLocationNumber() {
        this.Locations = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingLocation)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(location -> this.Locations.put(location.getKey(), location.getValue()));
        return this.Locations;

    }

    public Map<String, Long> CountSkillsNumber() {
        this.Skills = new LinkedHashMap<>();
        this.wuzzuf_dataset_strings
                .map(JobDaoSpark::preparingSkills)// getting companies
                .filter(StringUtils::isNotBlank) // removing  null values
                .countByValue()// retrieve each company and number  of occurrence as  a map
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(skill -> this.Skills.put(skill.getKey(), skill.getValue()));
        return this.Skills;
    }
    public Map<String, Long> CountSkillssNumber() {
        this.Skills = new LinkedHashMap<>();
        JavaRDD<String> EachSkill = this.wuzzuf_dataset_strings
                                 .map(JobDaoSpark::preparingSkillss)
                                 .filter (StringUtils::isNotBlank)
                                 .flatMap(skill->Arrays.asList (
                                         skill .split (",")).iterator ());
       EachSkill.countByValue().entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(skill -> this.Skills.put(skill.getKey(), skill.getValue()));


        return this.Skills;
    }


}
