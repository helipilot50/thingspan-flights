package com.objectivity.thingspan.examples.flights.visual;

import java.awt.Component;
import java.awt.EventQueue;

import javax.swing.JFrame;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.view.Viewer;
import org.graphstream.ui.view.View;
import java.awt.BorderLayout;
import javax.swing.JLabel;

public class GraphQueryWindow {

	private JFrame frmFlightsGraphViewer;
	private Graph graph;
	private Viewer viewer;
	private View view;

	/**
	 * Launch the window
	 */
	public static void show(final Graph graph) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					GraphQueryWindow window = new GraphQueryWindow(graph);
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
	public GraphQueryWindow(Graph graph) {
		this.graph = graph;
		this.viewer = new Viewer(graph, Viewer.ThreadingModel.GRAPH_IN_ANOTHER_THREAD);
		this.viewer.disableAutoLayout();
		this.view = viewer.addDefaultView(false);   // false indicates "no JFrame".

		initialize();
	}
	
	public Graph getGraph() {
		return graph;
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmFlightsGraphViewer = new JFrame();
		frmFlightsGraphViewer.setTitle("Flights graph viewer");
		frmFlightsGraphViewer.setBounds(100, 100, 450, 300);
		frmFlightsGraphViewer.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmFlightsGraphViewer.getContentPane().setLayout(new BorderLayout(0, 0));
		
		
		JLabel lblNewLabel = new JLabel("Flights");
		frmFlightsGraphViewer.getContentPane().add(lblNewLabel, BorderLayout.NORTH);

		frmFlightsGraphViewer.getContentPane().add((Component) view, BorderLayout.CENTER);

	}

}
