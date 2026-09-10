package com.tianji.aigc.agent;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.tianji.aigc.config.ToolResultHolder;
import com.tianji.aigc.constants.Constant;
import com.tianji.aigc.enums.ChatEventTypeEnum;
import com.tianji.aigc.service.ChatService;
import com.tianji.aigc.service.ChatSessionService;
import com.tianji.aigc.vo.ChatEventVO;
import com.tianji.common.utils.UserContext;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import reactor.core.publisher.Flux;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractAgent implements Agent {

    @Resource
    private ChatSessionService chatSessionService;
    @Resource
    private ChatClient chatClient;
    @Resource
    private ChatMemory chatMemory;
    @Resource
    private StringRedisTemplate stringRedisTemplate;


    // 输出结束的标记
    public static final ChatEventVO STOP_EVENT = ChatEventVO.builder().eventType(ChatEventTypeEnum.STOP.getValue()).build();

    // 存储大模型的生成状态，这里采用ConcurrentHashMap是确保线程安全
    // 目前的版本暂时用Map实现，如果考虑分布式环境的话，可以考虑用redis来实现
    //public static final Map<String, Boolean> GENERATE_STATUS = new ConcurrentHashMap<>();

    private static final String GENERATE_STATUS_KEY = "GENERATE_STATUS:";


    /**
     * 处理用户输入，生成大模型的输出结果
     *
     * @param question  用户输入的问题
     * @param sessionId 会话id
     * @return 大模型的输出结果
     */
    @Override
    public String process(String question, String sessionId) {
        // 获取用户id
        var userId = UserContext.getUser();
        var requestId = this.generateRequestId();

        //更新会话时间
        this.chatSessionService.update(sessionId, question, userId);

        return this.getChatClientRequest(sessionId, requestId, question)
                .call()
                .content();
    }

    /**
     * 处理用户输入，生成大模型的输出结果
     *
     * @param question  用户输入的问题
     * @param sessionId 会话id
     * @return 大模型的输出结果
     */
    public Flux<ChatEventVO> processStream(String question, String sessionId) {
        // 获取用户id
        var userId = UserContext.getUser();
        var requestId = this.generateRequestId();
        // 大模型输出内容的缓存器，用于在输出中断后的数据存储
        var outputBuilder = new StringBuilder();
        // 获取对话id
        var conversationId = ChatService.buildConversationId(sessionId, userId);

        var hashOps=this.stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);


        //更新会话时间
        this.chatSessionService.update(sessionId, question, userId);

        return this.getChatClientRequest(sessionId, requestId, question)
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
                        // 将已生成的完整文本加入参数map
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

    /**
     * 获取大模型请求对象
     *
     * @param sessionId 会话id
     * @param requestId 请求id
     * @param question  用户输入的问题
     * @return 大模型请求对象
     */
    private ChatClient.ChatClientRequestSpec getChatClientRequest(String sessionId, String requestId, String question) {
        return this.chatClient.prompt()
                .system(promptSystem -> promptSystem.text(this.systemMessage()).params(this.systemMessageParams()))
                .advisors(advisor -> advisor.advisors(this.advisors()).params(this.advisorParams(sessionId, requestId)))
                .tools(this.tools())
                .toolContext(this.toolContext(sessionId, requestId))
                .user(question);
    }

    /**
     * 保存停止输出的记录
     *
     * @param conversationId 对话id
     * @param content   大模型输出的内容
     */
    private void saveStopHistoryRecord(String conversationId, String content) {
        this.chatMemory.add(conversationId, new AssistantMessage(content));
    }

    /**
     * 生成请求id
     *
     * @return 请求id
     */
    private String generateRequestId() {
        return IdUtil.fastSimpleUUID();
    }

    /**
     * 获取大模型的参数列表
     *
     * @param sessionId 会话id
     * @param requestId 请求id
     * @return 参数列表
     */
    @Override
    public Map<String, Object> advisorParams(String sessionId, String requestId) {
        var conversationId = ChatService.buildConversationId(sessionId, UserContext.getUser());
        return Map.of(ChatMemory.CONVERSATION_ID, conversationId);
    }

    /**
     * 停止生成
     *
     * @param sessionId 会话id
     */
    @Override
    public void stop(String sessionId) {
        //GENERATE_STATUS.remove(sessionId);

        //Redis实现，移除标记
        var hashOps=this.stringRedisTemplate.boundHashOps(GENERATE_STATUS_KEY);
        hashOps.delete(sessionId);
    }
}
