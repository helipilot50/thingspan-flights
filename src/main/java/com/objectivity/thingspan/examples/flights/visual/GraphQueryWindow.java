package com.objectivity.thingspan.examples.flights.visual;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.EventQueue;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFormattedTextField;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import javax.swing.table.DefaultTableModel;
import javax.swing.text.DefaultFormatterFactory;
import javax.swing.text.MaskFormatter;

import org.apache.spark.api.java.JavaRDD;
import org.graphstream.graph.Edge;
import org.graphstream.graph.ElementNotFoundException;
import org.graphstream.graph.Graph;
import org.graphstream.graph.IdAlreadyInUseException;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.view.View;
import org.graphstream.ui.view.Viewer;

import com.objectivity.thingspan.examples.flights.model.Airport;
import com.objectivity.thingspan.examples.flights.model.Flight;

import scala.Tuple2;
import java.awt.Toolkit;
import java.awt.Dimension;
import javax.swing.JSpinner;
import javax.swing.JScrollPane;

public class GraphQueryWindow {

	private JFrame frmFlightsGraphViewer;
	private Graph graph;
	private Viewer viewer;
	private JPanel view;
	private JFormattedTextField origin;
	private JFormattedTextField toAirport;
	private JFormattedDateTextField beginDateTime;
	private JFormattedDateTextField endDateTime;
	private EasyVisual vis;
	private JSpinner degreeSpinner;
	private MaskFormatter airportFormatter;
	private JScrollPane scrollGraphView;

	/**
	 * Launch the window
	 */
	public static void show(final EasyVisual vis) {
		System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GraphQueryWindow window = new GraphQueryWindow(vis);
					window.frmFlightsGraphViewer.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public GraphQueryWindow(final EasyVisual vis) {
		this.vis = vis;
		this.graph = new MultiGraph("Flights");
		setUpGraph();
		this.viewer = new Viewer(graph, Viewer.ThreadingModel.GRAPH_IN_GUI_THREAD);//Viewer.ThreadingModel.GRAPH_IN_ANOTHER_THREAD);
		this.viewer.disableAutoLayout();
		this.view = viewer.addDefaultView(false);   // false indicates "no JFrame".
		  try {
			  airportFormatter = new MaskFormatter("UUU");
			  airportFormatter.setPlaceholderCharacter('_');
		  } catch (ParseException e) {
		    e.printStackTrace();
		  }
		initialize();
		
		//add graph
		view.setMinimumSize(new Dimension(700,500));
		view.setSize(new Dimension(700,500));
		scrollGraphView.setViewportView(view);
		view.setMinimumSize(new Dimension(700,500));
		view.setSize(new Dimension(700,500));

		frmFlightsGraphViewer.addWindowListener(new WindowAdapter(){
			@Override
			public void windowClosing(WindowEvent e) {
				vis.close();
				e.getWindow().dispose();
			}
		});

	}

	private void setUpGraph(){
		this.graph.addAttribute("ui.stylesheet","url(file:.//style/stylesheet)"); 
		this.graph.addAttribute("ui.quality"); 
		this.graph.addAttribute("ui.antialias");
		
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmFlightsGraphViewer = new JFrame();
		frmFlightsGraphViewer.setMinimumSize(new Dimension(800, 600));
		frmFlightsGraphViewer.setIconImage(Toolkit.getDefaultToolkit().getImage(GraphQueryWindow.class.getResource("/ObjyTriangleLogo.png")));
		frmFlightsGraphViewer.setTitle("Flights graph viewer");
		frmFlightsGraphViewer.setBounds(100, 100, 800, 600);
		frmFlightsGraphViewer.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmFlightsGraphViewer.getContentPane().setLayout(new BorderLayout(0, 0));

		scrollGraphView = new JScrollPane();
		frmFlightsGraphViewer.getContentPane().add(scrollGraphView, BorderLayout.CENTER);

		
		JPanel dialogPanel = new JPanel();
		frmFlightsGraphViewer.getContentPane().add(dialogPanel, BorderLayout.WEST);
		GridBagLayout gbl_dialogPanel = new GridBagLayout();
		gbl_dialogPanel.columnWidths = new int[]{0, 0};
		gbl_dialogPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0};
		gbl_dialogPanel.columnWeights = new double[]{1.0, 1.0};
		gbl_dialogPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		dialogPanel.setLayout(gbl_dialogPanel);

		JLabel lblFrom = new JLabel("Origin");
		GridBagConstraints gbc_lblFrom = new GridBagConstraints();
		gbc_lblFrom.anchor = GridBagConstraints.EAST;
		gbc_lblFrom.insets = new Insets(0, 0, 5, 5);
		gbc_lblFrom.gridx = 0;
		gbc_lblFrom.gridy = 0;
		dialogPanel.add(lblFrom, gbc_lblFrom);

		origin = new JFormattedTextField(airportFormatter);
		GridBagConstraints gbc_fromAirport = new GridBagConstraints();
		gbc_fromAirport.anchor = GridBagConstraints.WEST;
		gbc_fromAirport.insets = new Insets(0, 0, 5, 0);
		gbc_fromAirport.gridx = 1;
		gbc_fromAirport.gridy = 0;
		dialogPanel.add(origin, gbc_fromAirport);
		origin.setColumns(3);
		origin.setFocusLostBehavior(JFormattedTextField.PERSIST);
		
		JLabel lblDegree = new JLabel("Degree");
		GridBagConstraints gbc_lblDegree = new GridBagConstraints();
		gbc_lblDegree.anchor = GridBagConstraints.EAST;
		gbc_lblDegree.insets = new Insets(0, 0, 5, 5);
		gbc_lblDegree.gridx = 0;
		gbc_lblDegree.gridy = 1;
		dialogPanel.add(lblDegree, gbc_lblDegree);
		
		degreeSpinner = new JSpinner();
		GridBagConstraints gbc_degreeSpinner = new GridBagConstraints();
		gbc_degreeSpinner.anchor = GridBagConstraints.WEST;
		gbc_degreeSpinner.insets = new Insets(0, 0, 5, 0);
		gbc_degreeSpinner.gridx = 1;
		gbc_degreeSpinner.gridy = 1;
		degreeSpinner.setValue(1);
		dialogPanel.add(degreeSpinner, gbc_degreeSpinner);

		JLabel lblTo = new JLabel("Destination");
		GridBagConstraints gbc_lblTo = new GridBagConstraints();
		gbc_lblTo.anchor = GridBagConstraints.EAST;
		gbc_lblTo.insets = new Insets(0, 5, 5, 5);
		gbc_lblTo.gridx = 0;
		gbc_lblTo.gridy = 2;
		dialogPanel.add(lblTo, gbc_lblTo);

		toAirport = new JFormattedTextField(airportFormatter);
		GridBagConstraints gbc_toAirport = new GridBagConstraints();
		gbc_toAirport.anchor = GridBagConstraints.WEST;
		gbc_toAirport.insets = new Insets(0, 0, 5, 0);
		gbc_toAirport.gridx = 1;
		gbc_toAirport.gridy = 2;
		dialogPanel.add(toAirport, gbc_toAirport);
		toAirport.setColumns(3);
		toAirport.setFocusLostBehavior(JFormattedTextField.PERSIST);

		JLabel lblBegin = new JLabel("Departure");
		GridBagConstraints gbc_lblBegin = new GridBagConstraints();
		gbc_lblBegin.anchor = GridBagConstraints.EAST;
		gbc_lblBegin.insets = new Insets(0, 0, 5, 5);
		gbc_lblBegin.gridx = 0;
		gbc_lblBegin.gridy = 3;
		dialogPanel.add(lblBegin, gbc_lblBegin);

		beginDateTime = new JFormattedDateTextField();
		GridBagConstraints gbc_beginDateTime = new GridBagConstraints();
		gbc_beginDateTime.insets = new Insets(0, 0, 5, 0);
		gbc_beginDateTime.fill = GridBagConstraints.HORIZONTAL;
		gbc_beginDateTime.gridx = 1;
		gbc_beginDateTime.gridy = 3;
		dialogPanel.add(beginDateTime, gbc_beginDateTime);
		beginDateTime.setColumns(10);

		JLabel lblEnd = new JLabel("Arrival");
		GridBagConstraints gbc_lblEnd = new GridBagConstraints();
		gbc_lblEnd.anchor = GridBagConstraints.ABOVE_BASELINE_TRAILING;
		gbc_lblEnd.insets = new Insets(0, 0, 5, 5);
		gbc_lblEnd.gridx = 0;
		gbc_lblEnd.gridy = 4;
		dialogPanel.add(lblEnd, gbc_lblEnd);

		endDateTime = new JFormattedDateTextField();
		GridBagConstraints gbc_endDateTime = new GridBagConstraints();
		gbc_endDateTime.insets = new Insets(0, 0, 5, 0);
		gbc_endDateTime.fill = GridBagConstraints.HORIZONTAL;
		gbc_endDateTime.gridx = 1;
		gbc_endDateTime.gridy = 4;
		dialogPanel.add(endDateTime, gbc_endDateTime);
		endDateTime.setColumns(10);

		JButton btnGo = new JButton("Query");
		btnGo.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				executeQuery();
			}
		});
		GridBagConstraints gbc_btnGo = new GridBagConstraints();
		gbc_btnGo.gridwidth = 2;
		gbc_btnGo.insets = new Insets(0, 0, 5, 0);
		gbc_btnGo.fill = GridBagConstraints.BOTH;
		gbc_btnGo.gridx = 0;
		gbc_btnGo.gridy = 5;
		dialogPanel.add(btnGo, gbc_btnGo);
		
		JButton btnClear = new JButton("Clear");
		btnClear.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				graph.clear();
				setUpGraph();
			}
		});
		GridBagConstraints gbc_btnClear = new GridBagConstraints();
		gbc_btnClear.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnClear.gridwidth = 2;
		gbc_btnClear.gridx = 0;
		gbc_btnClear.gridy = 6;
		dialogPanel.add(btnClear, gbc_btnClear);
		

		
		
		
	}

	private void executeQuery(){
		final String from = origin.getText();
		final String to = toAirport.getText();
		final String lowDateTime = beginDateTime.getText();
		final String highDateTime = endDateTime.getText();
		final Integer degree = (Integer) degreeSpinner.getValue();
		EdgeWorker edgeWorker = new EdgeWorker(from, degree, to, lowDateTime, highDateTime);
		edgeWorker.execute();
	}

	public  class JFormattedDateTextField extends JFormattedTextField {
		private static final long serialVersionUID = -2709754451864626931L;
		Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm");

		public JFormattedDateTextField() {
			super();
			MaskFormatter maskFormatter = null;
			try {
				maskFormatter = new MaskFormatter("####/##/## ##:##");
			} catch (ParseException e) {
				e.printStackTrace();
			}

			maskFormatter.setPlaceholderCharacter('_');
			setFormatterFactory(new DefaultFormatterFactory(maskFormatter));
			this.addFocusListener(new FocusAdapter() {
				public void focusGained(FocusEvent e) {
					if (getFocusLostBehavior() == JFormattedTextField.PERSIST)
						setFocusLostBehavior(JFormattedTextField.COMMIT_OR_REVERT);
				}

				public void focusLost(FocusEvent e) {
					try {
						Date date = (Date) format.parseObject(getText());
						setValue(format.format(date));
					} catch (ParseException pe) {
						setFocusLostBehavior(JFormattedTextField.PERSIST);
						setText("");
						setValue(null);
					}
				}
			});
		}

		public void setValue(Date date) {
			super.setValue(toString(date));
		}

		private Date toDate(String sDate) {
			Date date = null;
			if (sDate == null) return null;
			try {
				date = (Date) format.parseObject(sDate);
			}
			catch (ParseException pe) {
				// ignore
			}

			return date;
		}

		private String toString(Date date) {
			try {
				return format.format(date);
			} catch (Exception e) {
				return "";
			}
		}
	}

	public class AirportsFlights {
		public List<Airport> airports;
		public List<Flight> flights;

		public AirportsFlights(List<Airport> airports, List<Flight> flights) {
			super();
			this.airports = airports;
			this.flights = flights;
		}
	}
	
	public class EdgeWorker extends SwingWorker<List<Flight>, Integer> {
		private String from;
		private String to;
		private String lowDateTime;
		private String highDateTime;
		private Integer degree;
		
		List<Flight> flights;
		List<Airport> airports;

		public EdgeWorker(String from, Integer degree, String to, String lowDateTime, String highDateTime){
			this.from = from.toUpperCase().replace("_", "");
			this.degree = degree;
			this.to = to.toUpperCase().replace("_", "");
			this.lowDateTime = lowDateTime.replace("/", "").replace(" ", "").replace(":", "");
			this.highDateTime = highDateTime.replace("/", "").replace(" ", "").replace(":", "");
		}
		@Override
		protected  List<Flight> doInBackground() throws Exception {
			Tuple2<JavaRDD<Flight>, JavaRDD<Airport>> chunks = vis.nodeEdgesFor(from, to, lowDateTime, highDateTime, degree);
			airports = chunks._2().collect();
			flights = chunks._1().collect();
			System.out.println("queried " );
			return flights;
			
		}

		protected void done() {
			if (this.airports == null || this.flights == null)
				return;
			System.out.println(String.format("display start AD:%d FL:%d", this.airports.size(), this.flights.size()) );
			for (Airport airport : this.airports){
				try{
					Node node = graph.addNode(airport.getIATA());//.asInstanceOf[MultiNode] 
					node.addAttribute("name", airport.getIATA());
					node.addAttribute("ui.label", airport.getIATA());
					node.addAttribute("ui.color", 1);
					int x = (int) (airport.getLongitude() * 10000);
					int y = (int) (airport.getLatitude() * 10000);
					node.setAttribute("xyz", x, y, 0);
				} catch (IdAlreadyInUseException e){

				}

			}
			for (Flight flight : this.flights){

				String fn = flight.getCarrier() + flight.getFlightNumber();
				try {
					Edge edge = graph.addEdge(flight.getFlightDate() + flight.getOrigin() + fn, flight.getOrigin(), flight.getDestination(), 
							true);

					edge.addAttribute("name", fn);
					edge.addAttribute("ui.label", fn);
					edge.addAttribute("ui.color", 0);
				} catch (ElementNotFoundException  e) {
				} catch (IdAlreadyInUseException e) {
				}
			}
			System.out.println("displayed " );
		}
	}

}
