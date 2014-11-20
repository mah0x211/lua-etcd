local inspect = require('util').inspect;
local clock = os.clock;

local function sandbox(f)
    local start = clock();
    -- create a coroutine and have it yield every 50 instructions
    local co = coroutine.create(f)
    
    debug.sethook( co, function( ... )
        print( start, clock(), clock() - start, inspect( {...} ) );
        -- coroutine.yield
        error('abort');
    end, "", 100000000 );

    -- demonstrate stepped execution, 5 'ticks'
    print( 'ret', inspect( { coroutine.resume(co) } ) );
end

sandbox(function()
    while 1 do
        --print("", "badfile")
    end
end);
