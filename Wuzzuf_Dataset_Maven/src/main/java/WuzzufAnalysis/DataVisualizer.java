package WuzzufAnalysis;

import java.awt.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;

public class DataVisualizer {


    public void BarChart(Map data, String Visualization_title, String X_axis, String Y_axis, String Series_Title, int Limit) {
        ArrayList<String> DataKeys = (ArrayList<String>) data.keySet().stream().limit(Limit).collect(Collectors.toList());
        ArrayList<Long> DataValues = (ArrayList<Long>) data.values().stream().limit(Limit).collect(Collectors.toList());
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title(Visualization_title).xAxisTitle(X_axis).yAxisTitle(Y_axis).build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideE);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        chart.addSeries(Series_Title, DataKeys, DataValues);

        new SwingWrapper(chart).displayChart();
    }

    public void PieChart(Map<String, Long> data, String Visualization_title, int Limit) {
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
        // Show it
        new SwingWrapper(chart).displayChart();

    }

}
