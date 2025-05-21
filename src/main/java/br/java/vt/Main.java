package br.java.vt;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class Main {
    private static String bootstrapServers;
    private static KafkaProducer<String, String> producer;
    private static KafkaConsumer<String, String> consumer;
    private static AdminClient adminClient;

    // Componentes de UI
    private static JFrame frame;
    private static DefaultMutableTreeNode root;
    private static JTree topicTree;
    private static JTable messageTable;
    private static DefaultTableModel tableModel;
    private static JTextField inputField;
    private static JButton sendButton, refreshButton, consumeButton, createTopicButton, deleteTopicButton;

    public static void main(String[] args) {
        setDarkTheme();
        solicitarBootstrapServers();
        configurarKafkaClients();
        SwingUtilities.invokeLater(Main::criarGUI);
    }

    // Configuração visual moderna
    private static void setDarkTheme() {
        UIManager.put("Panel.background", new Color(43, 43, 43));
        UIManager.put("Button.background", new Color(75, 110, 175));
        UIManager.put("TextField.background", new Color(60, 63, 65));
        UIManager.put("TextField.foreground", Color.WHITE);
        UIManager.put("TextArea.background", new Color(43, 43, 43));
        UIManager.put("TextArea.foreground", Color.WHITE);
        UIManager.put("Label.foreground", Color.WHITE);
        UIManager.put("OptionPane.background", new Color(43, 43, 43));
        UIManager.put("OptionPane.messageForeground", Color.WHITE);
        UIManager.put("ToolTip.background", new Color(60, 63, 65));
        UIManager.put("ToolTip.foreground", Color.WHITE);
        UIManager.put("Tree.background", new Color(43, 43, 43));

        // Configurações para JTable
        UIManager.put("Table.background", new Color(30, 30, 30));
        UIManager.put("Table.foreground", Color.WHITE);
        UIManager.put("Table.selectionBackground", new Color(60, 60, 60));
        UIManager.put("Table.selectionForeground", Color.WHITE);

        // Configurações para JTableHeader (cabeçalho da tabela)
        UIManager.put("TableHeader.background", new Color(45, 45, 45));
        UIManager.put("TableHeader.foreground", Color.WHITE);
    }

    private static void solicitarBootstrapServers() {
        do {
            bootstrapServers = JOptionPane.showInputDialog(null, "Informe o endereço do Kafka:", "Kafka Config", JOptionPane.QUESTION_MESSAGE);
            if (bootstrapServers == null) System.exit(0);
        } while (bootstrapServers.isEmpty());
    }

    private static void configurarKafkaClients() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);

        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "swing-client-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(props);

        adminClient = AdminClient.create(Map.of("bootstrap.servers", bootstrapServers));
    }

    private static void criarGUI() {
        frame = new JFrame("Kafka Swing Client");

        // Adiciona o ícone da janela
        ImageIcon icon = new ImageIcon(Objects.requireNonNull(Main.class.getResource("/icon.png")));
        frame.setIconImage(icon.getImage());
        // Adiciona o ícone da janela

        frame.setSize(1000, 650);
        frame.setLocationRelativeTo(null);
        frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                int result = JOptionPane.showConfirmDialog(frame, "Deseja sair?", "Sair", JOptionPane.YES_NO_OPTION);
                if (result == JOptionPane.YES_OPTION) {
                    consumer.close();
                    producer.close();
                    adminClient.close();
                    System.exit(0);
                }
            }
        });

        inicializarComponentes();
        adicionarListeners();
        frame.setVisible(true);
        atualizarTopicos();
    }

    private static void inicializarComponentes() {
        // Árvore de tópicos
        root = new DefaultMutableTreeNode("Tópicos Kafka");
        topicTree = new JTree(root);
        topicTree.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        JScrollPane topicScroll = new JScrollPane(topicTree);

        // Colunas da tabela
        String[] columns = {"Offset", "Key", "Value"};
        tableModel = new DefaultTableModel(columns, 0);
        messageTable = new JTable(tableModel);
        messageTable.setFont(new Font("Consolas", Font.PLAIN, 14));
        JScrollPane messageScroll = new JScrollPane(messageTable);

        // SplitPane moderno
        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, topicScroll, messageScroll);
        splitPane.setDividerLocation(300);
        splitPane.setResizeWeight(0.25);

        // Painel superior com botões
        refreshButton = new JButton("Atualizar");
        consumeButton = new JButton("Consumir");
        createTopicButton = new JButton("Criar Tópico");
        deleteTopicButton = new JButton("Excluir Tópico");
        JPanel topPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 10));
        for (JButton btn : List.of(refreshButton, consumeButton, createTopicButton, deleteTopicButton)) {
            btn.setFocusPainted(false);
            btn.setFont(new Font("Segoe UI", Font.BOLD, 13));
            topPanel.add(btn);
        }
        topPanel.setOpaque(false);

        // Painel inferior para envio de mensagens
        inputField = new JTextField();
        inputField.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        sendButton = new JButton("Enviar");
        sendButton.setFont(new Font("Segoe UI", Font.BOLD, 13));
        sendButton.setFocusPainted(false);
        JPanel bottomPanel = new JPanel(new BorderLayout(8, 0));
        bottomPanel.add(inputField, BorderLayout.CENTER);
        bottomPanel.add(sendButton, BorderLayout.EAST);
        bottomPanel.setBorder(BorderFactory.createEmptyBorder(10, 0, 10, 0));
        bottomPanel.setOpaque(false);

        // Montagem do painel principal
        JPanel mainPanel = new JPanel(new BorderLayout(0, 8));
        mainPanel.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
        mainPanel.add(topPanel, BorderLayout.NORTH);
        mainPanel.add(splitPane, BorderLayout.CENTER);
        mainPanel.add(bottomPanel, BorderLayout.SOUTH);

        frame.setContentPane(mainPanel);
    }

    private static void adicionarListeners() {
        refreshButton.addActionListener(e -> atualizarTopicos());

        sendButton.addActionListener(e -> {
            TreePath path = topicTree.getSelectionPath();
            if (path == null || path.getPathCount() < 2) return;
            String topic = path.getPathComponent(1).toString();
            String msg = inputField.getText().trim();
            if (!msg.isEmpty()) {
                producer.send(new ProducerRecord<>(topic, msg));
                inputField.setText("");
            }
        });

        consumeButton.addActionListener(e -> consumirMensagens());

        createTopicButton.addActionListener(e -> criarTopico());

        deleteTopicButton.addActionListener(e -> excluirTopico());
    }

    private static void atualizarTopicos() {
        root.removeAllChildren();
        try {
            Map<String, TopicDescription> topics = adminClient.describeTopics(adminClient.listTopics().names().get()).allTopicNames().get();
            for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
                DefaultMutableTreeNode topicNode = new DefaultMutableTreeNode(entry.getKey());
                for (TopicPartitionInfo p : entry.getValue().partitions()) {
                    topicNode.add(new DefaultMutableTreeNode("Partição " + p.partition()));
                }
                root.add(topicNode);
            }
            ((DefaultTreeModel) topicTree.getModel()).reload();
        } catch (Exception ex) {
            ex.printStackTrace();
            JOptionPane.showMessageDialog(frame, "Erro ao carregar tópicos: " + ex.getMessage());
        }
    }

    private static void consumirMensagens() {
        TreePath path = topicTree.getSelectionPath();
        if (path == null || path.getPathCount() < 2) return;
        String topic = path.getPathComponent(1).toString();
        tableModel.setRowCount(0);

        Executors.newVirtualThreadPerTaskExecutor().submit(() -> {
            try {
                consumer.unsubscribe();
                consumer.subscribe(List.of(topic));
                consumer.poll(Duration.ofMillis(0));
                Set<org.apache.kafka.common.TopicPartition> partitions;
                do {
                    Thread.sleep(100);
                    consumer.poll(Duration.ofMillis(100));
                    partitions = consumer.assignment();
                } while (partitions.isEmpty());

                consumer.seekToBeginning(partitions);
                boolean received = false;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> rec : records) {
                    received = true;
                    Object[] row = {rec.offset(), rec.key(), rec.value()};
                    SwingUtilities.invokeLater(() -> tableModel.addRow(row));
                }
                if (!received) {
                    SwingUtilities.invokeLater(() -> {
                        JOptionPane.showMessageDialog(frame, "Nenhuma mensagem recebida.", "Aviso", JOptionPane.INFORMATION_MESSAGE);
                    });
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(frame, "Erro ao consumir: " + ex.getMessage());
            }
        });
    }

    private static void criarTopico() {
        String name = JOptionPane.showInputDialog(frame, "Nome do novo tópico:");
        if (name == null || name.trim().isEmpty()) return;
        String parts = JOptionPane.showInputDialog(frame, "Número de partições:");
        if (parts == null || parts.trim().isEmpty()) return;
        int numPartitions;
        try {
            numPartitions = Integer.parseInt(parts.trim());
        } catch (NumberFormatException ex) {
            JOptionPane.showMessageDialog(frame, "Número de partições inválido.", "Erro", JOptionPane.ERROR_MESSAGE);
            return;
        }
        try {
            NewTopic newTopic = new NewTopic(name.trim(), numPartitions, (short) 1);
            adminClient.createTopics(List.of(newTopic)).all().get();
            atualizarTopicos();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                JOptionPane.showMessageDialog(frame, "Tópico já existe.");
            } else {
                JOptionPane.showMessageDialog(frame, "Erro: " + ex.getMessage());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void excluirTopico() {
        TreePath path = topicTree.getSelectionPath();
        if (path == null || path.getPathCount() < 2) return;
        String topic = path.getPathComponent(1).toString();
        int confirm = JOptionPane.showConfirmDialog(frame, "Excluir tópico '" + topic + "'?", "Confirmar", JOptionPane.YES_NO_OPTION);
        if (confirm == JOptionPane.YES_OPTION) {
            try {
                adminClient.deleteTopics(List.of(topic)).all().get();
                atualizarTopicos();
            } catch (Exception ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(frame, "Erro ao excluir tópico: " + ex.getMessage());
            }
        }
    }
}
