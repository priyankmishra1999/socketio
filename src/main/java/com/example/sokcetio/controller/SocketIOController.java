package com.example.sokcetio.controller;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.listener.ConnectListener;
import com.corundumstudio.socketio.listener.DataListener;
import com.corundumstudio.socketio.listener.DisconnectListener;
import com.corundumstudio.socketio.protocol.Packet;
import com.example.sokcetio.model.CoinData;
import com.example.sokcetio.model.Message;
import com.example.sokcetio.model.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Priyank Mishra
 */
@Component
@Log4j2
public class SocketIOController {

    @Autowired
    private SocketIOServer socketServer;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private static final ObjectMapper CONVERTER = new Jackson2JsonEncoder().getObjectMapper();


    SocketIOController(SocketIOServer socketServer) {
        this.socketServer = socketServer;

        this.socketServer.addConnectListener(onUserConnectWithSocket);
        this.socketServer.addDisconnectListener(onUserDisconnectWithSocket);

        /*
          Here we create only one event listener
          but we can create any number of listener
          messageSendToUser is socket end point after socket connection user have to send message payload on messageSendToUser event
         */
        this.socketServer.addEventListener("messageSendToUser", Message.class, onSendMessage);
        this.socketServer.addEventListener("coinData", CoinData.class, onDataSend);

    }


    public ConnectListener onUserConnectWithSocket = new ConnectListener() {
        @Override
        public void onConnect(SocketIOClient client) {
            log.info("Perform operation on user connect in controller");
        }
    };


    public DisconnectListener onUserDisconnectWithSocket = new DisconnectListener() {
        @Override
        public void onDisconnect(SocketIOClient client) {
            log.info("Perform operation on user disconnect in controller");
        }
    };

    public DataListener<Message> onSendMessage = new DataListener<Message>() {
        @Override
        public void onData(SocketIOClient client, Message message, AckRequest acknowledge) throws Exception {

            /*
              Sending message to target user
              target user should subscribe the socket event with his/her name.
              Send the same payload to user
             */

            log.info(message.getSenderName() + " user send message to user " + message.getTargetUserName() + " and message is " + message.getMessage());
            socketServer.getBroadcastOperations().sendEvent(message.getTargetUserName(), client, message);


            /*
              After sending message to target user we can send acknowledge to sender
             */
            acknowledge.sendAckData("Message send to target user successfully");
        }
    };
    public DataListener<CoinData> onDataSend = new DataListener<CoinData>() {
        @Override
        public void onData(SocketIOClient socketIOClient, CoinData coinData, AckRequest ackRequest) throws Exception {
            List<Map<String, Object>> maps = null;
            StringBuilder stringBuilder = null;
            if (coinData.getType().equals(Type.FULL)) {
                stringBuilder = new StringBuilder("select * from centralized_wallet_service.coin");
            } else {
                stringBuilder = new StringBuilder("select * from centralized_wallet_service.coin where coin_short_name='BTC'");
            }
            maps = jdbcTemplate.queryForList(String.valueOf(stringBuilder));
            socketServer.getBroadcastOperations().sendEvent(coinData.getType().toString(), maps);
            ackRequest.sendAckData("Message send to target user successfully");
            Data(maps, coinData.getType());
        }
    };

    public void Data(Object data, Type type) {

        executorService.scheduleAtFixedRate(() -> {
            try {
                kafkaTemplate.send("wallet_Data", CONVERTER.writeValueAsString(data));
            } catch (JsonProcessingException e) {
                log.info("error in send data to  kafka");
            }
        }, 0, 5000, TimeUnit.MILLISECONDS);
    }

    @KafkaListener(topics = "wallet_Data", groupId = "socketIOGroup")
    public void listenKafkaData(String data) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> list = mapper.readValue(data, new TypeReference<List<Map<String, Object>>>() {
        });
        socketServer.getBroadcastOperations().sendEvent("coinData", list);
    }

}