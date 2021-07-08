package com.example.WuzzufDataAnalysis;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.web.bind.annotation.RestController;


public class DataVisualizer {


    public void BarChart(Map data, String Visualization_title, String X_axis, String Y_axis, String Series_Title, int Limit,String Name) {
        ArrayList<String> DataKeys = (ArrayList<String>) data.keySet().stream().limit(Limit).collect(Collectors.toList());
        ArrayList<Long> DataValues = (ArrayList<Long>) data.values().stream().limit(Limit).collect(Collectors.toList());
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title(Visualization_title).xAxisTitle(X_axis).yAxisTitle(Y_axis).build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        chart.addSeries(Series_Title, DataKeys, DataValues);
        try {
            BitmapEncoder.saveBitmap(chart, "src/main/resources/static/"+Name, BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void PieChart(Map<String, Long> data, String Visualization_title, int Limit,String Name) {
        PieChart chart = new PieChartBuilder().width(800).height(600).title(Visualization_title).build();
        // Customize Chart
        Color[] sliceColors = new Color[]{
                new Color(48, 136, 174),
                new Color(114, 210, 235),
                new Color(191, 247, 255),
                new Color(22, 106, 142),
                new Color(42, 78, 108),
                new Color(60, 100, 150),
        };
        chart.getStyler().setSeriesColors(sliceColors);
        // Series

        for (Map.Entry<String, Long> entry : data.entrySet().stream().limit(Limit).collect(Collectors.toList())) {
            chart.addSeries(entry.getKey(), entry.getValue());
        }
        try {
            BitmapEncoder.saveBitmap(chart, "src/main/resources/static/"+Name, BitmapEncoder.BitmapFormat.PNG);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
