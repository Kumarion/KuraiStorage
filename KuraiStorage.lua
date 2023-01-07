local DEBUG = true;
local BASE_FLUSH_INTERVAL = 9;

local function debugPrint(...)
	if (DEBUG) then
		print(...);
	end;
end

local DataStoreService = game:GetService("DataStoreService");
local MemoryStoreService = game:GetService("MemoryStoreService");
local MessagingService = game:GetService("MessagingService");
local Concur = require(script.Parent.Packages.Concur);

local KuraiStorage = {
	_cache = {},
}

function KuraiStorage.new(name: string)
	-- Get existing store object if possible
	if (KuraiStorage._cache[name]) then
		debugPrint("Returning existing store for " .. name);
		return KuraiStorage._cache[name];
	end;

	-- Create new store object
	debugPrint("Creating new store for " .. name);
	local Store = {
		_dsStore = DataStoreService:GetDataStore(name),
		_msMap = MemoryStoreService:GetSortedMap(name),
		_cache = {},
		_msgId = "BS_" .. name,
		_updateQueue = {},
		_events = {},
		_destroyed = false,
		_currentlyFlushing = false,
		_flushesInProgress = {},
	};

	function Store:_flushQueue()
		self._currentlyFlushing = true;
		
		for key, transformers in pairs(self._updateQueue) do
			if (self._flushesInProgress[key]) then
				debugPrint("Flush already in progress for " .. key);
				continue;
			end;
			
			if (#transformers < 1) then
				debugPrint("No transformers for " .. key);
				continue;
			end;
			
			self._flushesInProgress[key] = true;

			task.spawn(function()
				-- UpdateAsync can fail if other servers are trying to access it at the same time
				local unlocked, lockWaitTime = false, 0;
				
				while (unlocked == false) do
					local success, message = pcall(function()
						debugPrint("Attempting to retrieve lock for " .. key);
						local CurrentJobId = self._msMap:GetAsync(key);
						
						if (CurrentJobId ~= nil) then
							debugPrint("Lock already taken by " .. CurrentJobId);
							unlocked = false;
							return nil;
						else
							unlocked = true;
						end;
						
						--// Make this server unlocked and Set the lock to this game id if its passed all those
						self._msMap:SetAsync(key, game.JobId, 15);
					end);
					
					if (not success) then
						warn(message);
					end;
					
					debugPrint("Is this server currently unlocked: ", unlocked);
					
					if (unlocked == false) then
						lockWaitTime += 0.1;
						
						if (lockWaitTime >= 40) then
							warn(
								"Update flush for "
									.. key
									.. " expired after ~60 seconds while waiting for lock to be available."
							);
							self._flushesInProgress[key] = nil;
							return;
						end;
						
						debugPrint("Waiting for lock for " .. key .. " for " .. lockWaitTime .. " seconds so far");
					end;
				end;
				
				debugPrint("Received lock for " .. key);

				-- Update the global value
				debugPrint("Updating global value for " .. key);
				
				--// We only want to call UpdateAsync if this server is unlocked (no other server modifying)
				self._dsStore:UpdateAsync(key, function(storedValue)
					local value = storedValue;

					for i, transformer in ipairs(transformers) do
						local success, newValue = pcall(transformer, value, true);
						
						if (not success) then
							warn(newValue);
							debugPrint("Cancelled transformer " .. i .. " on " .. key);
							continue; -- skip this one, transform errored
						end;

						if (newValue == nil) then
							debugPrint("Skipped transformer " .. i .. " on " .. key);
							continue; -- skip this one, transform exited
						end;

						debugPrint("Applied transformer " .. i .. " on " .. key);
						value = newValue;
					end;
					
					table.clear(transformers);
					self._cache[key] = value;

					-- Inform other servers they need to refresh
					task.defer(function()
						local publishSuccess, publishResult = pcall(function()
							debugPrint("Informing other servers of changes to " .. key);
							MessagingService:PublishAsync(self._msgId, {
								JobId = game.JobId,
								Key = key,
							});
						end);
						
						if (not publishSuccess) then
							warn(publishResult);
						end;
					end);

					return value;
				end);

				-- Unlock this key for the next server to take
				-- We want to unlock this key after 6 seconds or we risk the other server checking the key too quickly and hitting the 6 seconds rate limit
				local ConcurThread = Concur.delay(6, function()
					self._flushesInProgress[key] = nil;
					pcall(self._msMap.RemoveAsync, self._msMap, key);
				end);
				ConcurThread:OnCompleted(function(err)
					debugPrint("Unlocked " .. key .. " for this JobId");
				end);
			end);
		end;
		
		self._currentlyFlushing = false;
	end;

	function Store:GetKeyChangedSignal(key: string)
		local event = self._events[key];
		
		if (not event) then
			event = Instance.new("BindableEvent");
			self._events[key] = event;
		end;
		
		return event.Event;
	end

	function Store:Get(key: string, default: any?, skipCache: boolean?)
		if (not skipCache and self._cache[key] ~= nil) then
			debugPrint("Getting local value of " .. key);
			return self._cache[key] or default;
		end;

		debugPrint("Getting global value of " .. key);

		local value = self._dsStore:GetAsync(key);
		if (value == nil) then
			value = default;
		end;

		self._cache[key] = value;
		return value;
	end

	function Store:Update(key: string, transformer: (any?, boolean?) -> any?, canLocallyUpdate)
		-- Queue it up for updating on the latest real value & replication
		debugPrint("Queuing global transformer for " .. key);
		
		if (self._updateQueue[key] == nil) then
			self._updateQueue[key] = { transformer };
		else
			table.insert(self._updateQueue[key], transformer);
		end;

		-- First, perform it locally
		debugPrint("Applying local transformer for " .. key);
		local success, newValue = pcall(transformer, self._cache[key], false);
		
		if (not success) then
			warn(newValue);
			return; -- cancel, transform errored
		end;

		if (newValue == nil) then
			return; -- cancel, transform exited
		end;

		self._cache[key] = newValue;
		local event = self._events[key];
		
		if (event and canLocallyUpdate) then
			event:Fire(newValue);
		end;
	end

	function Store:Destroy()
		KuraiStorage._cache[name] = nil;

		self._destroyed = true;
		self:_flushQueue();

		for _, event in pairs(self._events) do
			event:Destroy();
		end;
		
		if (self._msgConnection ~= nil) then
			self._msgConnection:Disconnect();
		end;

		table.clear(self);
	end;

	task.spawn(function()
		-- Subscribe to store's msg for cross-server updates
		local subscribeSuccess, subscribeConnection = pcall(function()
			return MessagingService:SubscribeAsync(Store._msgId, function(message)
				if (game.JobId == message.Data.JobId) then
					return;
				end;

				local key = message.Data.Key;
				debugPrint(name, "/", key, "was updated by another server");

				local newValue = Store:Get(key, Store._cache[key], true);
				local event = Store._events[key];
				if (event) then
					event:Fire(newValue);
				end;
			end);
		end);
		
		if (subscribeSuccess) then
			Store._msgConnection = subscribeConnection;
		else
			warn(subscribeConnection);
		end;

		-- Start update queue flush thread
		while (not Store._destroyed) do
			local jitter = math.random(100, 400) / 100;
			task.wait(BASE_FLUSH_INTERVAL + jitter);

			debugPrint("Flushing update queue periodically");
			Store:_flushQueue();
		end;
	end);

	-- Cache the store object for future GetStore sharing
	KuraiStorage._cache[name] = Store;
	return Store;
end

function KuraiStorage.commitFlushQueues()
	for _, Store in pairs(KuraiStorage._cache) do
		task.spawn(Store._flushQueue, Store);
	end;
end

game:BindToClose(function()
	for _, Store in pairs(KuraiStorage._cache) do
		if (Store._currentlyFlushing == true) then
			--// If the store is already currently flushing, we want to wait 6 seconds (cooldown) until we flush the leaving queue
			local FlushTime = math.random(7, 8);
			task.delay(FlushTime, function()
				Store:_flushQueue();
			end);
		else
			Store:_flushQueue();
		end;
	end;
end)

return KuraiStorage;