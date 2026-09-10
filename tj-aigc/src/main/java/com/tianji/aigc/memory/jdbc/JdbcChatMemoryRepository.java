package com.tianji.aigc.memory.jdbc;

import cn.hutool.core.collection.CollStreamUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.stream.StreamUtil;
import cn.hutool.core.util.StrUtil;
import com.tianji.aigc.entity.ChatRecord;
import com.tianji.aigc.memory.MessageUtil;
import com.tianji.aigc.service.ChatRecordService;
import jakarta.annotation.Resource;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.sql.Wrapper;
import java.util.List;
import java.util.Set;

/**
 * 基于JDBC实现的ChatMemoryRepository
 */
public class JdbcChatMemoryRepository implements ChatMemoryRepository {

    // 默认redis中key的前缀
    public static final String DEFAULT_PREFIX = "CHAT:";

    private final String prefix;

    // 注入spring redis模板，进行redis的操作
    @Resource
    private ChatRecordService chatRecordService;

    public JdbcChatMemoryRepository() {
        this.prefix = DEFAULT_PREFIX;
    }

    public JdbcChatMemoryRepository(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public List<String> findConversationIds() {
        List<ChatRecord> chatRecordList = this.chatRecordService.lambdaQuery()
                .select(ChatRecord::getConversationId)
                .list();
        return CollStreamUtil.toList(chatRecordList, ChatRecord::getConversationId);
    }


    @Override
    public List<Message> findByConversationId(String conversationId) {
        List<ChatRecord> chatRecordList = this.chatRecordService.lambdaQuery()
                .eq(ChatRecord::getConversationId, conversationId)
                .orderByAsc(ChatRecord::getCreateTime)
                .list();
        return CollStreamUtil.toList(chatRecordList, chatRecord -> MessageUtil.toMessage(chatRecord.getData()));
    }

    @Override
    public void saveAll(String conversationId, List<Message> messages) {
        this.deleteByConversationId(conversationId);
        Long userId = Convert.toLong(StrUtil.subBefore(conversationId, "_", true));
        List<ChatRecord> chatRecordList = CollStreamUtil.toList(messages, message -> ChatRecord.builder()
                .conversationId(conversationId)
                .data(MessageUtil.toJson(message))
                .creater(userId)
                .updater(userId)
                .build());
        this.chatRecordService.saveBatch(chatRecordList);
    }

    @Override
    public void deleteByConversationId(String conversationId) {
        this.chatRecordService.lambdaUpdate().eq(ChatRecord::getConversationId, conversationId).remove();
    }

    private String getKey(String conversationId) {
        return prefix + conversationId;
    }
}
