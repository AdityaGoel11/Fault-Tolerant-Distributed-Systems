#!lua name=mylib

local function add_wc(keys, args)
    
    local word_count = args[1]
    local wc = cjson.decode(word_count)

    local already_ACKed = redis.call('XACK', 'files', args[2], args[3])
    if already_ACKed == 1 then
        for word, count in pairs(wc) do
            redis.call('ZINCRBY', 'count', count, word)
        end
        redis.call('XACK', 'files', args[2], args[3])
    else

    end
    return 0
end    

redis.register_function('add_wc', add_wc)