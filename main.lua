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

local object

local function mqtt_client_event_cbfunc(mqtt_client, event, data, payload, metas, property)
    log.info("mqtt_cbfunc", "event")
    if event == "connack" then
        sys.publish("connack")
    elseif event == "recv" then
        local topic = data
        -- local topic_alias = property.topic_alias
        log.info("pub topic", topic)
        log.info("pub payload", payload)
        for k, v in pairs(property) do
            log.info("k, v", k, v)
        end
    end
end

local will = {
    payload = "this client is dead",
    topic = will_topic,
    retain = 1,
    qos = 0,
    property = {
        delay_interval = 200
    }
}

local property = {
    topic_alias_max_len = 60000
}

-- local will = nil
sys.taskInit(function()
    sys.waitUntil("IP_READY")
    -- 创建一个mqtt5 client
    object = mqtt_user.create(client_id, username, password, keepalive, clean_session, will, property)

    -- 注册用户回调
    object:on(mqtt_client_event_cbfunc)

    -- 连接服务器
    object:connect(host, 1883)

    -- 等待连接成功
    sys.waitUntil("connack")

    -- 订阅主题
    -- object:subscribe("SubTest", 2)
    object:subscribe(subscribe_topic_qos0, 0)
    object:subscribe(subscribe_topic_qos1, 1)
    object:subscribe(subscribe_topic_qos2, 2)

    -- 往主题发布数据
    object:publish(publish_topic_qos0, "" .. os.time(), 0, 1, pubproperty0)
    -- object:publish(publish_topic_qos1, "" .. os.time(), 1, 1, pubproperty1)
    -- object:publish(publish_topic_qos2, "" .. os.time(), 2, 1, pubproperty2)
    sys.wait(1000)
    while 1 do
        -- 往主题发布数据
        object:publish("", "" .. os.time(), 0, 1, pubproperty0)
        -- object:publish("", "" .. os.time(), 1, 1, pubproperty1)
        -- object:publish("", "" .. os.time(), 2, 1, pubproperty2)
        sys.wait(1000)
    end
end)

sys.run()
