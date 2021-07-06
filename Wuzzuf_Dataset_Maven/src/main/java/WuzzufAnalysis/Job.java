package WuzzufAnalysis;

import java.util.ArrayList;

public class Job {
    String Title;
    String Company;
    String Location;
    String Type;
    String Level;
    String YearsExp;
    String[] Skills;


    public Job(String title, String company, String location, String type, String level, String yearsExp, String[] skills) {
        Title = title;
        Company = company;
        Location = location;
        Type = type;
        Level = level;
        YearsExp = yearsExp;
        Skills = skills;
    }


    public String getTitle() {
        return Title;
    }

    public String getCompany() {
        return Company;
    }

    public String getLocation() {
        return Location;
    }

    public String getType() {
        return Type;
    }

    public String getLevel() {
        return Level;
    }

    public String getYearsExp() {
        return YearsExp;
    }

    public String[] getSkills() {
        return Skills;
    }


}
