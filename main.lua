local mqtt_user = require "mqtt_user"
-- local mqtt_user = require "mqtt5"
local host = "imadaydreamer.cn"
local port = 1883
local client_id = "12345678"
local username = "username"
local password = "hahahaha"
local keepalive = 240
local clean_session = 1

local publish_topic_qos0 = "publish_topic_qos0"
local publish_topic_qos1 = "publish_topic_qos1"
local publish_topic_qos2 = "publish_topic_qos2"

local subscribe_topic_qos0 = "subscribe_topic_qos0"
local subscribe_topic_qos1 = "subscribe_topic_qos1"
local subscribe_topic_qos2 = "subscribe_topic_qos2"

local pubproperty0 = {
    topic_alias = 2000
}
local pubproperty1 = {
    topic_alias = 2001
}
local pubproperty2 = {
    topic_alias = 2002
}

local will_topic = "will"
local will_payload = "this client is dead"
local will_qos = 0
local will_retain = 0
local will_property = {
    delay_interval = 200
}

local connect_property = {
    topic_alias_max_len = 60000
}

local mqttc

local function mqtt_client_event_cbfunc(mqtt_client, event, data, payload, metas, property)
    log.info("mqtt_cbfunc", "event")
    if event == "connack" then
        sys.publish("connack")
    elseif event == "recv" then
        local topic = data
        log.info("pub topic", topic)
        log.info("pub payload", payload)
    end
    if property and type(property) == "table" then
        for k, v in pairs(property) do
            log.info("k, v", k, v)
        end
    end
end

-- local will = nil
sys.taskInit(function()
    sys.waitUntil("IP_READY")
    -- 创建一个mqtt5 client
    mqttc = mqtt_user.create(nil, host, port)
    mqttc:auth(client_id, username, password)

    -- 注册用户回调
    mqttc:on(mqtt_client_event_cbfunc)
    mqttc:will(will_topic, will_payload, will_qos, will_retain, will_property)

    -- 连接服务器
    mqttc:connect(connect_property)

    -- 等待连接成功
    sys.waitUntil("connack")

    -- 订阅主题
    mqttc:subscribe(subscribe_topic_qos0, 0)
    mqttc:subscribe(subscribe_topic_qos1, 1)
    mqttc:subscribe(subscribe_topic_qos2, 2)

    -- 往主题发布数据
    mqttc:publish(publish_topic_qos0, "" .. os.time(), 0, 1, pubproperty0)
    -- mqttc:publish(publish_topic_qos1, "" .. os.time(), 1, 1, pubproperty1)
    -- mqttc:publish(publish_topic_qos2, "" .. os.time(), 2, 1, pubproperty2)
    sys.wait(1000)
    while 1 do
        -- 往主题发布数据
        mqttc:publish("", "" .. os.time(), 0, 1, pubproperty0)
        -- mqttc:publish("", "" .. os.time(), 1, 1, pubproperty1)
        -- mqttc:publish("", "" .. os.time(), 2, 1, pubproperty2)
        sys.wait(1000)
    end
end)

sys.run()
