local mqtt5 = {}


mqtt5.event_connack = 1
mqtt5.event_publish = 2
mqtt5.event_puback = 3
mqtt5.event_pubcomp = 4
mqtt5.event_suback = 5
mqtt5.event_unsuback = 6
mqtt5.event_pingresp = 7
mqtt5.event_disconnect = 8

-- #define MQTT_MSG_RELEASE 		0	/**< mqtt 释放资源前回调消息 */
-- #define MQTT_MSG_CLOSE 			4	/**< mqtt 关闭回调消息(不会再重连) */
-- #define MQTT_MSG_CON_ERROR 		5
-- #define MQTT_MSG_TX_ERROR 		6
-- #define MQTT_MSG_CONACK_ERROR 	7
-- #define MQTT_MSG_NET_ERROR 		8
-- #define MQTT_MSG_CONN_TIMEOUT   9	/**< mqtt 连接超时回调消息 */

local ConnectFixHead = 0x10
local ConnackFixHead = 0x20
local PublishFixHead = 0x30
local PubackFixHead = 0x40
local PubrecFixHead = 0x50
local PubrelFixHead = 0x60
local PubcompFixHead = 0x70
local SubscribeFixHead = 0x80
local SubackFixHead = 0x90
local UnsubscribeFixHead = 0xA0
local UnsubackFixHead = 0xB0
local PingReqFixHead = 0xC0
local PingRespFixHead = 0xD0
local DisconnectFixHead = 0xE0
local AuthFixHead = 0xF0

-- Properties
local Format = 0x01
local MessageTimeout = 0x02
local ContentType = 0x03
local ResponseTopic = 0x08
local CorrelationData = 0x09
local DefineIdentifiers = 0x0B
local SessionExpireInterval = 0x11
local UserIdentifiers = 0x12
local ServerKeepAlive = 0x13
local AuthMethod = 0x15
local AuthData = 0x16
local RequestIssueMseeage = 0x17
local WillDelayInterval = 0x18
local RequestResponseMessage = 0x19
local RequestMessage = 0x1A
local ServerReference = 0x1C
local ReasonString = 0x1F
local RecvMaxLen = 0x21
local TopicAliasMaxLen = 0x22
local TopicAlias = 0x23
local MaxQos = 0x24
local RetainPropertyAvailability = 0x25
local UserProperty = 0x26
local PayloadMaxLen = 0x27
local WildcardSubsAvailability = 0x28
local SubIdentifiersAvailability = 0x29
local ShareSubAvailability = 0x2A


local function decode_twobyte(buf)
    local data, param1 = string.unpack(">H", buf)
    return data
end


local function encode_len(len)
    local s = ""
    local digit
    repeat
        digit = len % 128
        len = (len - digit) / 128
        if len > 0 then
            digit = bit.bor(digit, 0x80)
        end
        s = s .. string.char(digit)
    until (len <= 0)
    return s
end

local function decode_len(pos, buf)
    local multiplier = 1
    local length = 0
    repeat
        if pos > #buf then
            return false
        end
        local digit = string.byte(buf, pos)
        length = length + ((digit % 128) * multiplier)
        multiplier = multiplier * 128
        pos = pos + 1
    until digit < 128
    return true, length, pos
end

local MqttPropertyAnalysis = {
    -- 0x01
    [Format] = function(buf, result)
        local data = buf:byte(2, 2)
        result.format = data
        return buf:sub(3)
    end,
    -- 0x02
    [MessageTimeout] = function(buf, result)
        local data = string.unpack(">L", buf:sub(2, 5))
        result.message_timeout = data
        return buf:sub(6)
    end,
    -- 0x03
    [ContentType] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.content_type = data
        return buf:sub(len + 4)
    end,
    -- 0x08
    [ResponseTopic] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.response_topic = data
        return buf:sub(len + 4)
    end,
    -- 0x09
    [CorrelationData] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.correlation_data = data
        return buf:sub(len + 4)
    end,
    -- 0x0B
    [DefineIdentifiers] = function(buf, result)
        local ret, length, pos = false, 0, 2
        ret, length, pos = decode_len(pos, buf)
        result.define_identifiers = length
        return buf:sub(pos + 1)
    end,
    -- 0x11
    [SessionExpireInterval] = function(buf, result)
        local time = string.unpack(">L", buf:sub(2, 5))
        result.expire_interval = time
        return buf:sub(6)
    end,
    -- 0x12
    [UserIdentifiers] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.user_identifiers = data
        return buf:sub(len + 4)
    end,
    -- 0x13
    [ServerKeepAlive] = function(buf, result)
        local data = decode_twobyte(buf:sub(2, 3))
        result.server_keep_alive = data
        return buf:sub(4)
    end,
    -- 0x15
    [AuthMethod] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.auth_method = data
        return buf:sub(len + 4)
    end,
    -- 0x16
    [AuthData] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.auth_data = data
        return buf:sub(len + 4)
    end,
    -- 0x17
    [RequestIssueMseeage] = function(buf, result)
        local data = buf:byte(2, 2)
        result.req_issue_message = data
        return buf:sub(3)
    end,
    -- 0x18
    [WillDelayInterval] = function(buf, result)
        local time = string.unpack(">L", buf:sub(2, 5))
        result.will_delay_interval = time
        return buf:sub(6)
    end,
    -- 0x19
    [RequestResponseMessage] = function(buf, result)
        local data = buf:byte(2, 2)
        result.req_response_message = data
        return buf:sub(3)
    end,
    -- 0x1A
    [RequestMessage] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.request_message = data
        return buf:sub(len + 4)
    end,
    -- 0x1C
    [ServerReference] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.server_reference = data
        return buf:sub(len + 4)
    end,
    -- 0x1F
    [ReasonString] = function(buf, result)
        local len = string.unpack(">H", buf:sub(2, 3))
        local data = buf:sub(4, len + 3)
        result.reason_string = data
        return buf:sub(len + 4)
    end,
    -- 0x21
    [RecvMaxLen] = function(buf, result)
        local data = decode_twobyte(buf:sub(2, 3))
        result.recv_max_len = data
        return buf:sub(4)
    end,
    -- 0x22
    [TopicAliasMaxLen] = function(buf, result)
        local data = decode_twobyte(buf:sub(2, 3))
        result.topic_alias_max_len = data
        return buf:sub(4)
    end,
    -- 0x23
    [TopicAlias] = function(buf, result)
        local data = decode_twobyte(buf:sub(2, 3))
        result.topic_alias = data
        return buf:sub(4)
    end,
    -- 0x24
    [MaxQos] = function(buf, result)
        local data = buf:byte(2, 2)
        result.max_qos = data
        return buf:sub(3)
    end,
    -- 0x25
    [RetainPropertyAvailability] = function(buf, result)
        local data = buf:byte(2, 2)
        result.retain_property = data
        return buf:sub(3)
    end,
    -- 0x26
    [UserProperty] = function(buf, result)
        local find_key = true
        local len, key, value
        
        -- remove flag
        buf = buf:sub(2)

        -- proc key
        len = string.unpack(">H", buf:sub(1, 2))
        key = buf:sub(3, len + 2)
        buf = buf:sub(len + 3)

        -- proc value
        len = string.unpack(">H", buf:sub(1, 2))
        value = buf:sub(3, len + 2)
        result.user_property[key] = value
        return buf:sub(len + 3)
    end,
    -- 0x27
    [PayloadMaxLen] = function(buf, result)
        local data = string.unpack(">L", buf:sub(2, 5))
        result.payload_max_len = data
        return buf:sub(6)
    end,
    -- 0x28
    [WildcardSubsAvailability] = function(buf, result)
        local data = buf:byte(2, 2)
        result.sub_wildcard = data
        return buf:sub(3)
    end,
    -- 0x29
    [SubIdentifiersAvailability] = function(buf, result)
        local data = buf:byte(2, 2)
        result.sub_identifiers = data
        return buf:sub(3)
    end,
    -- 0x2A
    [ShareSubAvailability] = function(buf, result)
        local data = buf:byte(2, 2)
        result.sub_share = data
        return buf:sub(3)
    end
}

local function property_analysis(buf, result)
    result.user_property = {}
    while #buf > 0 do
        local flag = buf:byte(1, 1)
        -- log.info("flag", flag)
        if MqttPropertyAnalysis[flag] then
            buf = MqttPropertyAnalysis[flag](buf, result)
        else
            -- 不存在的属性, 直接抛弃这包数据吧
            break
        end
    end
end

local MqttPublicAnalysis = {
    [ConnackFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        local session = data:byte(3)
        local reason_code = data:byte(4)
        pos = pos + 2
        local property
        local property_len = string.byte(data, pos, pos)
        if property_len > 0 then
            local buf = string.sub(data, pos + 1, pos + property_len)
            log.info("connack  pro", buf:toHex())
            property = {}
            property_analysis(buf, property)
        end
        local param = {
            session = session,
            reason_code = reason_code,
            property = property
        }
        log.info("Connack session", param.session, "reason", param.reason_code, pos)
        event_cb(mqtt5.event_connack, user_param, param)
        return return_data
    end,
    [PublishFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        local qos = data:byte(1, 1) & 0x06
        -- 主题
        local topicLen = string.sub(data, pos, pos + 1)
        topicLen = tonumber(topicLen:toHex(), 16)
        log.info("topic len", topicLen)
        pos = pos + 2
        local topic = string.sub(data, pos, pos + topicLen)
        pos = pos + topicLen

        -- 如果qos大于0, 则有2bytes标识符
        local identifier
        if qos > 0 then
            identifier = string.sub(data, pos, pos + 1)
            pos = pos + 2
        end
        -- 属性，如果有的话
        local property
        local property_len = string.byte(data, pos, pos)
        if property_len > 0 then
            local buf = string.sub(data, pos + 1, pos + property_len)
            -- log.info("sub  pro", buf:toHex())
            property = {}
            property_analysis(buf, property)
        end
        pos = pos + 1 + property_len
        -- 负载
        local payload = string.sub(data, pos)
        local param = {
            topic = topic,
            payload = payload,
            qos = qos,
            identifier = identifier,
            property = property
        }
        event_cb(mqtt5.event_publish, user_param, param)
        return return_data
    end,
    [SubackFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        log.info("sub ", data:toHex(), length, pos)
        local len = string.sub(data, pos, pos + 1)
        return return_data
    end,

    [PingRespFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        event_cb(mqtt5.event_pingresp, user_param)
        return return_data
    end,
    [PubrelFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        return return_data
    end,
    [PubrecFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        local identifier = data:sub(3, 4)
        local param = {
            identifier = identifier
        }
        event_cb(mqtt5.event_pubrec, user_param, param)
        return return_data
    end,

    [DisconnectFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        event_cb(mqtt5.event_disconnect, user_param)
        return return_data
    end
}

function mqtt5.ping_req()
    local str = string.char(PingReqFixHead, 0x00)
    return str
end

function mqtt5.pack_puback(id)
    local str = string.char(PubackFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end
function mqtt5.pack_pubrec(id)
    local str = string.char(PubrecFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end

function mqtt5.pack_pubrel(id)
    local str = string.char(PubrelFixHead, 0x04) .. id .. string.char(0x00, 0x00)
    return str
end

function mqtt5.mqtt_proc(event_cb, buf, user_param)
    log.info("mqtt recv", buf:sub(1, 50):toHex())
    local fix_head = buf:byte(1)
    local result, length, pos = false, 0, 2
    result, length, pos = decode_len(pos, buf)

    if not result then
        return false, buf
    end

    if #buf < length + pos - 1 then
        log.info("data length not enough", #buf, length + 2)
        return false, buf
    end
    -- fix_head = (fix_head & 0xF0) >> 4
    if MqttPublicAnalysis[fix_head & 0xF0] then
        return true, MqttPublicAnalysis[fix_head & 0xF0](event_cb, buf, length, pos, user_param)
    else
        log.info("id unregister", string.char(fix_head):toHex())
        return true, buf:sub(length + pos)
    end
end

function mqtt5.pack_connect(client_id, username, password, keepAlive, clean_session, will, property)
    local str = ""
    --- 固定报头
    -- str = str .. string.char(0x10)
    --- 可变报头
    -- 协议名
    -- MSB LSB M Q T T
    str = str .. string.char(0x00, 0x04) .. "MQTT"

    -- 协议版本 5
    str = str .. string.char(0x05)

    -- 连接标志
    -- bit7 username 
    -- bit6 password
    -- bit5 will_retain
    -- bit4、3 will_qos
    -- bit2 will flag
    -- bit1 clean start
    -- bit0 : reserved
    client_id = client_id and client_id or ""
    username = username and username or ""
    password = password and password or ""

    local connect_flag = (#username == 0 and 0 or 1) * 128 + (#password == 0 and 0 or 1) * 64 + (clean_session or 1) * 2
    if will and type(will) == "table" then
        connect_flag = connect_flag + will.retain * 32 + will.qos * 8 + 4
    end

    str = str .. string.pack(">BH", connect_flag, keepAlive)

    --- properties
    local properties = ""
    -- 主题别名最大长度
    if property and type(property) == "table" then
        if property.topic_alias_max_len then
            properties = properties .. string.pack(">BH", TopicAliasMaxLen, property.topic_alias_max_len)
        end
    end

    str = str .. encode_len(#properties) .. properties

    --- payload
    if client_id and #client_id > 0 then
        str = str .. string.pack(">H", #client_id) .. client_id
    end

    -- will properties
    local will_data = ""
    if will and type(will) == "table" then
        properties = ""
        properties = string.char(Format, 0x01)

        if will.property and type(will.property) == "table" then
            if will.property.delay_interval then
                local delay = string.pack(">BL", 0x18, will.property.delay_interval)
                properties = properties .. delay
            end
        end
        str = str .. encode_len(#properties) .. properties
        str = str .. string.pack(">H", #will.topic) .. will.topic
        str = str .. string.pack(">H", #will.payload) .. will.payload
    end

    -- username
    if username and #username > 0 then
        str = str .. string.pack(">H", #username) .. username
    end

    -- password
    if password and #password > 0 then
        str = str .. string.pack(">H", #password) .. password
    end

    -- 长度
    str = string.char(ConnectFixHead) .. encode_len(#str) .. str
    log.info("tx data", str:toHex())
    return str
end

function mqtt5.pack_subscribe(topic, qos)
    local str = ""
    --- 固定报头
    str = str .. string.char(SubscribeFixHead)
    -- 用户属性
    local property = ""
    property = string.char(0x00, 0x00, 0x00)

    topic = string.pack(">H", #topic) .. topic

    local option = string.char(qos)

    str = str .. encode_len(#property + #topic + #option) .. property .. topic .. qos
    return str
end

function mqtt5.pack_publish(topic, payload, qos, retain, packet_id, property)
    local str = ""
    local topic_len = 0
    local identifier = ""
    local dup = 0
    if qos == 0 then
        qos = 0
    elseif qos == 1 then
        qos = 2
    elseif qos == 2 then
        qos = 4
    end
    if qos > 0 then
        identifier = string.pack(">H", packet_id)
    end
    --- publish 报头
    str = str .. string.char(PublishFixHead + (dup + qos + retain))
    -- log.info("packet head", str:toHex())
    -- TOPIC NAME
    if topic and #topic > 0 then
        topic = string.pack(">H", #topic) .. topic
        topic_len = #topic
    else
        topic = string.char(0x00, 0x00)
        topic_len = 2
    end

    --- publish 属性
    local properties = ""
    -- 载荷格式指示 UTF8
    local protocol = string.char(Format, 0x01)
    -- properties = string.char(#protocol) .. protocol
    properties = properties .. protocol
    -- 消息过期间隔 TODO

    -- 主题别名
    if property.topic_alias then
        properties = properties .. string.pack(">BH", TopicAlias, property.topic_alias)
    end
    properties = encode_len(#properties) .. properties
    str = str .. encode_len(topic_len + #identifier + #properties + #payload) .. topic .. identifier .. properties .. payload
    return str
end

return mqtt5
