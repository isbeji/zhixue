package com.tianji.aigc.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.tianji.aigc.config.SystemPromptConfig;
import com.tianji.aigc.config.ToolResultHolder;
import com.tianji.aigc.constants.Constant;
import com.tianji.aigc.enums.ChatEventTypeEnum;
import com.tianji.aigc.service.ChatService;
import com.tianji.aigc.service.ChatSessionService;
import com.tianji.aigc.vo.ChatEventVO;
import com.tianji.common.utils.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.vectorstore.QuestionAnswerAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "tj.ai", name = "chat-type", havingValue = "ENHANCE")//增强型智能体
public class ChatServiceImpl implements ChatService {

    private final ChatClient chatClient;
    private final SystemPromptConfig systemPromptConfig;
    private final ChatMemory chatMemory;
    private final StringRedisTemplate stringRedisTemplate;
    private final VectorStore vectorStore;
    private final ChatSessionService chatSessionService;

    // 存储大模型的生成状态，这里采用ConcurrentHashMap是确保线程安全
    // 目前的版本暂时用Map实现，如果考虑分布式环境的话，可以考虑用redis来实现
    //private static final Map<String, Boolean> GENERATE_STATUS = new ConcurrentHashMap<>();

    private static final String GENERATE_STATUS_KEY = "GENERATE_STATUS:";

    // 输出结束的标记
    private static final ChatEventVO STOP_EVENT = ChatEventVO.builder().eventType(ChatEventTypeEnum.STOP.getValue()).build();

    @Override
    public Flux<ChatEventVO> chat(String question, String sessionId) {
        // 获取用户id
        var userId = UserContext.getUser();
        // 获取对话id
        var conversationId = ChatService.buildConversationId(sessionId, userId);
        var outputBuilder = new StringBuilder();
        var hashOps=this.stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);

        var requestId= IdUtil.simpleUUID();

        // 创建RAG增强
        var qaAdvisor = QuestionAnswerAdvisor.builder(this.vectorStore)
                .searchRequest(SearchRequest.builder().similarityThreshold(0.6d).topK(6).build())
                .build();

        // 更新会话标题及时间
        this.chatSessionService.update(sessionId, question, userId);

        return this.chatClient.prompt()
                .system(promptSystem -> promptSystem
                        .text(this.systemPromptConfig.getChatSystemMessage().get()) // 设置系统提示语
                        .param("now", DateUtil.now()) // 设置当前时间的参数
                )
                .advisors(advisor -> advisor
                        .advisors(qaAdvisor)// 设置RAG增强
                        .param(ChatMemory.CONVERSATION_ID, conversationId))// 设置对话记忆中的对话id
                .toolContext(Map.of(Constant.REQUEST_ID, requestId, Constant.USER_ID, userId)) //通过工具上下文传递参数
                .user(question)
                .stream()
                .chatResponse()
                .doFirst(//() -> GENERATE_STATUS.put(sessionId, true)) // 第一次输出内容时执行

                        //使用redis实现， 开始生成时，在 Redis 设置标记
                        () -> hashOps.put(sessionId, "true")
                )
                .doOnError(throwable -> hashOps.delete(sessionId)) // 出现异常时，删除标识
                .doOnComplete(() -> hashOps.delete(sessionId)) // 完成时执行，删除标识
                .doOnCancel(() -> {
                    // 当输出被取消时，保存输出的内容到历史记录中
                    this.saveStopHistoryRecord(conversationId, outputBuilder.toString());
                })
                .takeWhile(response -> { // 通过返回值来控制Flux流是否继续，true：继续，false：终止
                    //return GENERATE_STATUS.getOrDefault(sessionId, false);

                    return hashOps.get(sessionId) != null;
                })
                .map(chatResponse -> {
                    // 对于响应结果进行处理，如果是最后一条数据，就把此次消息id放到内存中
                    // 主要用于存储消息数据到 redis中，可以根据消息di获取的请求id，再通过请求id就可以获取到参数列表了
                    // 从而解决，在历史聊天记录中没有外参数的问题
                    var finishReason = chatResponse.getResult().getMetadata().getFinishReason();
                    if (StrUtil.equals(Constant.STOP, finishReason)) {
                        var messageId = chatResponse.getMetadata().getId();
                        ToolResultHolder.put(messageId, Constant.REQUEST_ID, requestId);
                    }

                    // 获取大模型的输出的内容
                    String text = chatResponse.getResult().getOutput().getText();
                    // 追加到输出内容中
                    outputBuilder.append(text);

                    // 封装响应对象
                    return ChatEventVO.builder()
                            .eventData(text)
                            .eventType(ChatEventTypeEnum.DATA.getValue())
                            .build();
                })
                .concatWith(Flux.defer(()->{
                    // 通过请求id获取到参数列表，如果不为空，就将其追加到返回结果中
                    var map = ToolResultHolder.get(requestId);
                    if (CollUtil.isNotEmpty(map)) {
                        Map<String, Object> newMap = new HashMap<>(map);
                        newMap.put("text", outputBuilder.toString());
                        ToolResultHolder.remove(requestId); // 清除参数列表

                        // 响应给前端的参数数据
                        var chatEventVO = ChatEventVO.builder()
                                .eventData(newMap)
                                .eventType(ChatEventTypeEnum.PARAM.getValue())
                                .build();
                        return Flux.just(chatEventVO, STOP_EVENT);
                    }
                    return Flux.just(STOP_EVENT);
                }))
                .concatWith(Flux.just(ChatEventVO.builder()  // 标记输出结束
                        .eventType(ChatEventTypeEnum.STOP.getValue())
                        .build()));
    }

    @Override
    public void stop(String sessionId) {
        // 移除标记
        //GENERATE_STATUS.remove(sessionId);

        //Redis实现，移除标记
        var hashOps=this.stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);
        hashOps.delete(sessionId);
    }

    /**
     * 保存停止输出的记录
     *
     * @param conversationId 会话id
     * @param content        大模型输出的内容
     */
    private void saveStopHistoryRecord(String conversationId, String content) {
        this.chatMemory.add(conversationId, new AssistantMessage(content));
    }
}
