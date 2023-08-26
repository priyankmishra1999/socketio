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
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Log4j2
public class SocketIOController {

    @Autowired
    private SocketIOServer socketServer;
    @Autowired
    JdbcTemplate jdbcTemplate;

    SocketIOController(SocketIOServer socketServer){
        this.socketServer=socketServer;

        this.socketServer.addConnectListener(onUserConnectWithSocket);
        this.socketServer.addDisconnectListener(onUserDisconnectWithSocket);

        /**
         * Here we create only one event listener
         * but we can create any number of listener
         * messageSendToUser is socket end point after socket connection user have to send message payload on messageSendToUser event
         */
        this.socketServer.addEventListener("messageSendToUser", Message.class, onSendMessage);
        this.socketServer.addEventListener("coinData", CoinData.class,onDataSend);

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

            /**
             * Sending message to target user
             * target user should subscribe the socket event with his/her name.
             * Send the same payload to user
             */

            log.info(message.getSenderName()+" user send message to user "+message.getTargetUserName()+" and message is "+message.getMessage());
            socketServer.getBroadcastOperations().sendEvent(message.getTargetUserName(),client, message);


            /**
             * After sending message to target user we can send acknowledge to sender
             */
            acknowledge.sendAckData("Message send to target user successfully");
        }
    };
    public DataListener<CoinData> onDataSend = new DataListener<CoinData>() {
        @Override
        public void onData(SocketIOClient socketIOClient, CoinData coinData, AckRequest ackRequest) throws Exception {
            List<Map<String, Object>> maps=null;
            StringBuilder stringBuilder=null;
            if(coinData.getType().equals(Type.FULL)) {
                stringBuilder = new StringBuilder("select * from centralized_wallet_service.coin");
            }else {
                stringBuilder = new StringBuilder("select * from centralized_wallet_service.coin where coin_short_name='BTC'");
            }
            maps=jdbcTemplate.queryForList(String.valueOf(stringBuilder));

//            log.info(maps);
            socketServer.getBroadcastOperations().sendEvent(coinData.getType().toString(), maps);
            ackRequest.sendAckData("Message send to target user successfully");

        }
    };

}