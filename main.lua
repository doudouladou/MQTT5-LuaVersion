-- local mqtt_user = require "mqtt_user"
local mqtt_user = require "mqtt5"
local host = "imadaydreamer.cn"
local port = 1883
local client_id = "12345678"
local username = ""
local password = ""
local keepalive = 240
local clean_session = 1

local publish_topic_qos0 = "publish_topic_qos0"
local publish_topic_qos1 = "publish_topic_qos1"
local publish_topic_qos2 = "publish_topic_qos2"

local subscribe_topic_qos0 = "subscribe_topic_qos0"
local subscribe_topic_qos1 = "subscribe_topic_qos1"
local subscribe_topic_qos2 = "subscribe_topic_qos2"

local pubproperty0 = {
    alias = 2000
}
local pubproperty1 = {
    alias = 2001
}
local pubproperty2 = {
    alias = 2002
}

local will_topic = "will"

local object

local function mqtt_client_event_cbfunc(mqtt_client, event, data, payload, metas)
    -- log.info("mqtt_client_event_cbfunc", mqtt_client, event, data, payload)
    if event == "connack" then
        sys.publish("connack")
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
    -- object:subscribe(subscribe_topic_qos1, 1)
    -- object:subscribe(subscribe_topic_qos2, 2)

    -- 往主题发布数据
    object:publish(publish_topic_qos0, "" .. os.time(), 0, 1, pubproperty0)
    -- object:publish(publish_topic_qos1, "" .. os.time(), 1, 1, pubproperty1)
    -- object:publish(publish_topic_qos2, "" .. os.time(), 2, 1, pubproperty2)
    sys.wait(1000)
    while 1 do
        -- 往主题发布数据
        -- object:publish("", "" .. os.time(), 0, 1, pubproperty0)
        -- object:publish("", "" .. os.time(), 1, 1, pubproperty1)
        -- object:publish("", "" .. os.time(), 2, 1, pubproperty2)
        sys.wait(1000)
    end
end)

sys.run()
