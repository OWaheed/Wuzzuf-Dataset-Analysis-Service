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

@RestController
@RequestMapping(path = "api/")
public class JobDaoSpark {

    String path = "src/main/resources/Wuzzuf_Jobs.csv";
    SparkConf configuration= new SparkConf().setAppName("WuzzufDataAnalysis").setMaster("local[3]");
    JavaSparkContext sparkContext=new JavaSparkContext(configuration);
    JavaRDD<String> wuzzuf_dataset_strings= sparkContext.textFile(path).distinct();



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



}
