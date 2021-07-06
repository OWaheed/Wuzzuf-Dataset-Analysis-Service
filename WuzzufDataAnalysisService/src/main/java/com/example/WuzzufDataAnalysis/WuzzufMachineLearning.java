package com.example.WuzzufDataAnalysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class WuzzufMachineLearning {
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
                "    <td>Read Dataset ad convert it to dataframe </td>\n" +
                "    <td><a href=\"#\">Reading DataSet </a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Display Sample of the  dataframe </td>\n" +
                "    <td><a href=\"http://localhost:8080/SampleOfData\">Display Sample of DataSet </a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Display Structure  of the  dataframe </td>\n" +
                "    <td><a href=\"http://localhost:8080/StructureOfData\">Display Structure of DataSet</a></td>\n" +
                "    \n" +
                "  </tr>\n" +

                "  <tr>\n" +
                "    <td>Count the jobs for each coutry and display  that  in  order</td>\n" +
                "    <td><a href=\"http://localhost:8080/api/CountEachCompanyJobs\">Each Company offers Number</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Show Companny Pie Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/CompaniesPie\"> Company offers pie chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Count the jobs for each coutry and display  that  in  order</td>\n" +
                "    <td><a href=\"http://localhost:8080/api/CountEachCompanyJobs\">Each Company offers Number</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Find out what is the most popular job  titles?</td>\n" +
                "    <td><a href=\"http://localhost:8080/api/CountJobsNumber\">Number Of Offers For Each Title</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Companies num of  offers Bar Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/CompaniesPie\">pie chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Find out the  most popular areas</td>\n" +
                "    <td><a href=\"http://localhost:8080/api/CountLocationNumber\">Number Of Offers For Each Area</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>popular areasBar Chart</td>\n" +
                "    <td><a href=\"http://localhost:8080/AreasBarChart\">Bar chart</a></td>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>print Skills one   by one ordering by most needed  skills first</td>\n" +
                "    <td><a href=\"http://localhost:8080/api/CountSkillsNumber\">Most Important Skills</a></td>\n" +
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

    @RequestMapping(path = "SampleOfData")
    public String  SampleOfData()
    {   StringBuilder m = new StringBuilder();
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
                "<h2>Wuzzuf Dataset Sample</h2>\n" +
                "\n" +
                "<table>\n" +
                "  <tr>\n" +
                "    <th>Title</th>\n" +
                "    <th>Company</th>\n" +
                "    <th>Location</th>\n" +
                "    <th>Type</th>\n" +
                "    <th>Level</th>\n" +
                "    <th>YearsExp</th>\n" +
                "    <th>Country</th>\n" +
                "    <th>Skills</th>\n" +
                "    \n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Software Developer</td>\n" +
                "    <td>Jubia Smart Solutions</td>\n" +
                "    <td>Maadi</td>\n" +
                "    <td>Full Time</td>\n" +
                "    <td>Experienced</td>\n" +
                "    <td>2+ Yrs of Exp</td>\n" +
                "    <th>Cairo</th>\n" +
                "    <th>Software Development, Information Technology (IT), Computer Science, Software Engineering, Telecom, Angular, .NET, MySQL, Java</th>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Senior Sales Representative</td>\n" +
                "    <td>Byoot Bay</td>\n" +
                "    <td>Giza</td>\n" +
                "    <td>Full Time</td>\n" +
                "    <td>Experienced</td>\n" +
                "    <td>2-5 Yrs of Exp</td>\n" +
                "    <td>Cairo</td>\n" +
                "    <td>Females Only, Customer Service, Telesales, English, CRM, Sales Skills, Sales Target, Sales/Retail</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Junior Business Development Specialist (out door)</td>\n" +
                "    <td>E3mel Business for Financial & Managerial Consultancy</td>\n" +
                "    <td>Maadi</td>\n" +
                "    <td>Full Time</td>\n" +
                "    <td>Entry Level</td>\n" +
                "    <td>1-3 Yrs of Exp</td>\n" +
                "    <td>Cairo</td>\n" +
                "    <td>Sales Target, Sales Skills, B2B Sales, B2C Sales, Business Development, CRM, Marketing, Corporate Sales, Business Development</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Receptionist</td>\n" +
                "    <td>Value</td>\n" +
                "    <td> New Cairo</td>\n" +
                "    <td>Full Time</td>\n" +
                "    <td>Entry Level</td>\n" +
                "    <td>1-3 Yrs of Exp</td>\n" +
                "    <td>Cairo</td>\n" +
                "    <td>Office Management, Admin, Administration</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Stores Supervisor/Inventory Controller</td>\n" +
                "    <td>Confidential</td>\n" +
                "    <td>Cairo</td>\n" +
                "    <td>Full Time</td>\n" +
                "    <td>Experienced</td>\n" +
                "    <td>3-5 Yrs of Exp</td>\n" +
                "    <td>Egypt</td>\n" +
                "    <td>Logistics, Inventory Control, Purchasing, Logistics/Supply Chain, Purchasing/Procurement</td>\n" +
                "  </tr>\n" +


                "</table>\n" +
                "\n" +
                "</body>\n" +
                "</html>");
        return m.toString();
    }
    @RequestMapping(path = "StructureOfData")
    public String StructureOfData()
    {   StringBuilder m = new StringBuilder();
        m.append("<body>\n" +
            "\n" +
            "<h2>Wuzzuf Dataset Sample</h2>\n" +
            "\n" +
            "<table>\n" +
            "  <tr>\n" +
            "    <th>Column Name</th>\n" +
            "    <th>Column Type</th>\n" +
            "  </tr>\n" +

            "  <tr>\n" +
            "    <td>Title</td>\n" +
            "    <td>string (nullable = true)</td>\n" +
            "  </tr>\n" +

            "  <tr>\n" +
            "    <td>Company</td>\n" +
            "    <td>string (nullable = true)</td>\n" +
            "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Location</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Type</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Level</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>YearsExp</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Country</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "  <tr>\n" +
                "    <td>Skills</td>\n" +
                "    <td>string (nullable = true)</td>\n" +
                "  </tr>\n" +
                "</table>\n" +
            "\n" +
            "</body>\n" +
            "</html>");
        return m.toString();
    }
    @RequestMapping(path = "CompaniesPie")
    public String JobsPieChart(){
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most demanding Companies  For jobs?</h2>\n" +
                "<img src=\"src/main/resources/MostDemandigCompanies.JPG\">"+
                "</body>\n" +
                "</html>");

        return m.toString();


    }
    @RequestMapping(path = "SkillsBarChart")
    public String SkillsBarChart(){
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most Needed Skills </h2>\n" +
                "<img src=\"src/main/resources/MostNeededSkills.JPG\">"+
                "</body>\n" +
                "</html>");

        return m.toString();


    }
    @RequestMapping(path = "AreasBarChart")
    public String AreasBarChart(){
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most Popular Areas </h2>\n" +
                "<img src=\"src/main/resources/MostPopularAreas.JPG\">"+
                "</body>\n" +
                "</html>");

        return m.toString();
    }
    @RequestMapping(path = "TitlesBarChart")
    public String TitlesBarChart(){
        StringBuilder m = new StringBuilder();
        m.append("<html>\n" +"<head>"+"\n" +
                "</head>");
        m.append("<body>\n" +
                "\n" +
                "<h2>What are the  most Popular Job Titles </h2>\n" +
                "<img src=\"src/main/resources/MostPopularJobs.JPG\" alt=\"imageoftitles\">"+
                "</body>\n" +
                "</html>");

        return m.toString();
    }





}
