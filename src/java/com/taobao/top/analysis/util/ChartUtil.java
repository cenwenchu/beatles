package com.taobao.top.analysis.util;

import java.awt.Color;
import java.awt.Font;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot3D;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

/**
 * 绘图工具类
 * @author sihai
 *
 */
public class ChartUtil {
	
	/**
	 * 绘制饼图
	 * @param name
	 * @param outputFileName
	 * @param data
	 */
	public static void drawPie(String name, String outputFileName, List<Entry> data) {
		PieDataset dataset = makePieDataSet(data);  
		JFreeChart chart = ChartFactory.createPieChart3D(name, dataset, true, true, false);  
		PiePlot3D  plot = (PiePlot3D)chart.getPlot();  
		// 图片中显示百分比:默认方式  
		//plot.setLabelGenerator(new           StandardPieSectionLabelGenerator(StandardPieToolTipGenerator.DEFAULT_TOOLTIP_FORMAT));  
		// 图片中显示百分比:自定义方式，{0} 表示选项， {1} 表示数值， {2} 表示所占比例 ,小数点后两位  
		plot.setLabelGenerator(new StandardPieSectionLabelGenerator("{0}={1}({2})", NumberFormat.getNumberInstance(), new DecimalFormat("0.00%")));   
		// 图例显示百分比:自定义方式， {0} 表示选项， {1} 表示数值， {2} 表示所占比例                  
		plot.setLegendLabelGenerator(new StandardPieSectionLabelGenerator("{0}={1}({2})"));   
		// 设置背景色为白色   
		chart.setBackgroundPaint(Color.white);   
		// 指定图片的透明度(0.0-1.0)   
		plot.setForegroundAlpha(1.0f);   
		// 指定显示的饼图上圆形(false)还椭圆形(true)   
		plot.setCircular(true);   
		// 设置图标题的字体   
		Font font = new Font("黑体",Font.CENTER_BASELINE,20);   
		TextTitle title = new TextTitle(name);   
		title.setFont(font);    
		chart.setTitle(title);   
		FileOutputStream output = null;   
		try {   
			output = new FileOutputStream(outputFileName);   
		    ChartUtilities.writeChartAsJPEG(output, 100, chart, 640, 480, null);   
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 绘制柱状图
	 * @param name
	 * @param xName
	 * @param yName
	 * @param outputFileName
	 * @param data
	 */
	public static void drawBar(String name, String xName, String yName, String outputFileName, List<Entry> data) {
		JFreeChart chart = ChartFactory.createBarChart(name, 	// 图表标题
		        xName, // 目录轴的显示标签
		        yName, // 数值轴的显示标签
		        makeCategoryDataset(data), // 数据集
		        PlotOrientation.VERTICAL, // 图表方向：水平、垂直
		        true, 	// 是否显示图例(对于简单的柱状图必须是false)
		        false, 	// 是否生成工具
		        false 	// 是否生成URL链接
		);
		/*chart.getTitle().setFont((new Font("宋体", Font.CENTER_BASELINE, 20)));
	    chart.getLegend().setItemFont(new Font("宋体", Font.CENTER_BASELINE, 15));
	    Font labelFont = new Font("SansSerif", Font.TRUETYPE_FONT, 12);

	    chart.setAntiAlias(false);
	    chart.setBackgroundPaint(Color.WHITE);
	    // create plot
	    CategoryPlot plot = chart.getCategoryPlot();
	    // 设置横虚线可见
	    plot.setRangeGridlinesVisible(true);
	    // 虚线色彩
	    plot.setRangeGridlinePaint(Color.gray);

	    // 设置柱图背景色（注意，系统取色的时候要使用16位的模式来查看颜色编码，这样比较准确）
	    plot.setBackgroundPaint(new Color(160, 160, 255));

	    // 数据轴精度
	    NumberAxis vn = (NumberAxis) plot.getRangeAxis();
	    // vn.setAutoRangeIncludesZero(true);
	    DecimalFormat df = new DecimalFormat("#0.00");
	    vn.setNumberFormatOverride(df); // 数据轴数据标签的显示格式
	    // x轴设置
	    CategoryAxis domainAxis = plot.getDomainAxis();
	    domainAxis.setLabelFont(labelFont);// 轴标题
	    domainAxis.setTickLabelFont(labelFont);// 轴数值

	    // Lable（Math.PI/3.0）X轴文字的倾斜度
	    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.createUpRotationLabelPositions(Math.PI / 5.0));

	    domainAxis.setMaximumCategoryLabelWidthRatio(0.6f);// 横轴上的 Lable 是否完整显示

	    // 设置距离图片左端距离
	    domainAxis.setLowerMargin(0.1);
	    // 设置距离图片右端距离
	    domainAxis.setUpperMargin(0.1);

	    plot.setDomainAxis(domainAxis);

	    // y轴设置
	    ValueAxis rangeAxis = plot.getRangeAxis();
	    rangeAxis.setLabelFont(labelFont);
	    rangeAxis.setTickLabelFont(labelFont);
	    // 设置最高的一个 Item 与图片顶端的距离
	    rangeAxis.setUpperMargin(0.15);
	    // 设置最低的一个 Item 与图片底端的距离
	    rangeAxis.setLowerMargin(0.15);
	    plot.setRangeAxis(rangeAxis);

	    BarRenderer renderer = new BarRenderer();
	    // 设置柱子宽度
	    renderer.setMaximumBarWidth(0.05);
	    // 设置柱子高度
	    renderer.setMinimumBarLength(0.2);
	    // 设置柱子边框颜色
	    renderer.setBaseOutlinePaint(Color.BLACK);
	    // 设置柱子边框可见
	    renderer.setDrawBarOutline(true);

	    // // 设置柱的颜色
	    // renderer.setSeriesPaint(0, new Color(204, 255, 255));
	    // renderer.setSeriesPaint(1, new Color(153, 204, 255));
	    // renderer.setSeriesPaint(2, new Color(51, 204, 204));

	    // 设置每个地区所包含的平行柱的之间距离
	    renderer.setItemMargin(0.0);

	    // 显示每个柱的数值，并修改该数值的字体属性
	    renderer.setBaseItemLabelGenerator(new StandardCategoryItemLabelGenerator());
	    renderer.setBaseItemLabelsVisible(true);

	    plot.setRenderer(renderer);
	    // 设置柱的透明度
	    plot.setForegroundAlpha(1.0f);*/
	    
		FileOutputStream output = null;   
		try {   
			output = new FileOutputStream(outputFileName);   
		    ChartUtilities.writeChartAsJPEG(output, 100, chart, 1024, 800, null);   
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 绘制折线图
	 * @param name
	 * @param xName
	 * @param yName
	 * @param outputFileName
	 * @param multiLineData
	 */
	public static void drawLine(String name, String xName, String yName, String outputFileName, Map<String, List<LineEntry>> multiLineData) {
		XYSeriesCollection seriesCollection = new XYSeriesCollection();
		for(Map.Entry<String, List<LineEntry>> line : multiLineData.entrySet()) {
			XYSeries series = new XYSeries(line.getKey());
			for(LineEntry entry : line.getValue()) {
				series.add(entry.x, entry.y);
			}
			seriesCollection.addSeries(series);
		}
		
		JFreeChart chart = ChartFactory.createXYLineChart(name, xName,  
				yName, seriesCollection, PlotOrientation.VERTICAL, true,  
                true, false);
		FileOutputStream output = null;   
		try {   
			output = new FileOutputStream(outputFileName);   
		    ChartUtilities.writeChartAsJPEG(output, 100, chart, 1024, 800, null);   
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static PieDataset makePieDataSet(List<Entry> data) {
		
		DefaultPieDataset dataset = new DefaultPieDataset();
		for(Entry entry : data) {
			dataset.setValue(entry.key, entry.value);
		}
		return dataset;
	}
	
	private static CategoryDataset makeCategoryDataset(List<Entry> data) {
		
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		for(Entry entry : data) {
			dataset.setValue(entry.value, "", entry.key);
		}
		return dataset;
	}
	
	/**
	 * 
	 * @author sihai
	 *
	 */
	public static class Entry {
		public String key;
		public Number value;
		
		public Entry(String key, Number value) {
			this.key = key;
			this.value = value;
		}
	}
	
	/**
	 * 
	 * @author sihai
	 *
	 */
	public static class LineEntry {
		public Number x;
		public Number y;
		
		public LineEntry(Number x, Number y) {
			this.x = x;
			this.y = y;
		}
	}
}
