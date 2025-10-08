local mqtt5 = {}

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
local RelatedData = 0x09
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

mqtt5.event_connack = 1
mqtt5.event_publish = 2
mqtt5.event_pubrec = 3
mqtt5.event_disconnect = 4

local function decode_byte(buf)
    return buf:sub(2, 2), buf:sub(3)
end

local function decode_twobyte(buf)
    local data, param1 = string.unpack(">H", buf)
    log.info("test", data, param1)
    return data
end

local function decode_fourbyte()

end

local function decode_bin()

end

local function decode_utf8()

end

local function decode_variable_len()

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
    [Format] = function(buf, result)
        return
    end,
    [MessageTimeout] = function(buf, result)
        return
    end,
    [ContentType] = function(buf, result)
        return
    end,
    [ResponseTopic] = function(buf, result)
        return
    end,
    [RelatedData] = function(buf, result)
        return
    end,
    [DefineIdentifiers] = function(buf, result)
        return
    end,
    [SessionExpireInterval] = function(buf, result)
        return
    end,
    [UserIdentifiers] = function(buf, result)
        return
    end,
    [ServerKeepAlive] = function(buf, result)
        return
    end,
    [AuthMethod] = function(buf, result)
        return
    end,
    [AuthData] = function(buf, result)
        return
    end,
    [RequestIssueMseeage] = function(buf, result)
        return
    end,
    [WillDelayInterval] = function(buf, result)
        return
    end,
    [RequestResponseMessage] = function(buf, result)
        return
    end,
    [RequestMessage] = function(buf, result)
        return
    end,
    [ServerReference] = function(buf, result)
        return
    end,
    [ReasonString] = function(buf, result)
        return
    end,
    [RecvMaxLen] = function(buf, result)
        return
    end,
    [TopicAliasMaxLen] = function(buf, result)
        return
    end,
    [TopicAlias] = function(buf, result)
        local data = decode_twobyte(buf:sub(2, 3))
        result.topic_alias = data
        return buf:sub(4)
    end,
    [MaxQos] = function(buf, result)
        return
    end,
    [RetainPropertyAvailability] = function(buf, result)
        return
    end,
    [UserProperty] = function(buf, result)
        return
    end,
    [PayloadMaxLen] = function(buf, result)
        return
    end,
    [WildcardSubsAvailability] = function(buf, result)
        return
    end,
    [SubIdentifiersAvailability] = function(buf, result)
        return
    end,
    [ShareSubAvailability] = function(buf, result)
        return
    end
}

local MqttPublicAnalysis = {
    [ConnackFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        local param = {
            session = data:byte(3),
            reason_code = data:byte(4)
        }
        log.info("Connack session", param.session, "reason", param.reason_code)
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
            log.info("sub  pro", buf:toHex())
            property = {}
            while #buf > 0 do
                local flag = buf:byte(1, 1)
                if MqttPropertyAnalysis[flag] then
                    buf = MqttPropertyAnalysis[flag](buf, property)
                end
            end
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
        log.info("PingRespFixHead ", data:toHex(), length, pos)
        return return_data
    end,
    [PubrelFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        log.info("PubrelFixHead ", data:toHex(), length, pos)
        return return_data
    end,
    [PubrecFixHead] = function(event_cb, data, length, pos, user_param)
        local return_data = data:sub(length + pos)
        log.info("PubrecFixHead ", data:toHex(), length, pos)
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
    str = str .. encode_len(topic_len + #identifier + #properties + #payload) .. topic .. identifier .. properties ..
              payload
    return str
end

return mqtt5
