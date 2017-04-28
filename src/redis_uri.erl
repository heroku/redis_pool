%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 2006-2009. All Rights Reserved.
%% 
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% %CopyrightEnd%
%%
%%
-module(redis_uri).
-export([parse/1]).

%%%=========================================================================
%%%  API
%%%=========================================================================
parse(AbsURI) ->
    case parse_scheme(AbsURI) of
	{error, Reason} ->
	    {error, Reason};
	{Scheme, Rest} ->
	    case (catch parse_uri_rest(Scheme, Rest)) of
		{User, Pass, Host, Port, Path, Query} ->
		    {Scheme, User, Pass, Host, Port, Path, Query};
		_  ->
		    {error, {malformed_url, AbsURI}}    
	    end
    end.

%%%========================================================================
%%% Internal functions
%%%========================================================================
parse_scheme(AbsURI) ->
    case split_uri(AbsURI, ":", {error, no_scheme}, 1, 1) of
	{error, no_scheme} ->
	    {error, no_scheme};
	{StrScheme, Rest} ->
	    case list_to_atom(http_util:to_lower(StrScheme)) of
		Scheme when Scheme == http; Scheme == https; Scheme == redis ->
		    {Scheme, Rest};
		Scheme ->
		    {error, {not_supported_scheme, Scheme}}
	    end
    end.

parse_uri_rest(Scheme, "//" ++ URIPart) ->

    {Authority, PathQuery} = 
	case split_uri(URIPart, "/", URIPart, 1, 0) of
	    Split = {_, _} ->
		Split;
	    URIPart ->
		case split_uri(URIPart, "\\?", URIPart, 1, 0) of
		    Split = {_, _} ->
			Split;
		    URIPart ->
			{URIPart,""}
		end
	end,
    
    {UserInfo, HostPort} = split_uri(Authority, "@", {"", Authority}, 1, 1),
    [User,Pass] =
        case string:tokens(UserInfo, ":") of
            [U,P] -> [U,P];
            [UP] ->
                case [UserInfo, lists:reverse(UserInfo)] of
                    [_, ":" ++ _] -> [UP, ""];
                    [":" ++ _, _] -> ["", UP];
                    _ -> ["", UP]
                end;
            _ ->
                ["",""]
        end,
    {Host, Port} = parse_host_port(Scheme, HostPort),
    {Path, Query} = parse_path_query(PathQuery),
    {User, Pass, Host, Port, Path, Query}.


parse_path_query(PathQuery) ->
    {Path, Query} =  split_uri(PathQuery, "\\?", {PathQuery, ""}, 1, 0),
    {path(Path), Query}.
    

parse_host_port(Scheme,"[" ++ HostPort) -> %ipv6
    DefaultPort = default_port(Scheme),
    {Host, ColonPort} = split_uri(HostPort, "\\]", {HostPort, ""}, 1, 1),
    {_, Port} = split_uri(ColonPort, ":", {"", DefaultPort}, 0, 1),
    {Host, int_port(Port)};

parse_host_port(Scheme, HostPort) ->
    DefaultPort = default_port(Scheme),
    {Host, Port} = split_uri(HostPort, ":", {HostPort, DefaultPort}, 1, 1),
    {Host, int_port(Port)}.
    
split_uri(UriPart, SplitChar, NoMatchResult, SkipLeft, SkipRight) ->
    case re:run(UriPart, SplitChar, [{capture, first}]) of
        {match, [{Match, _}]} ->
            {string:substr(UriPart, 1, Match + 1 - SkipLeft),
             string:substr(UriPart, Match + 1 + SkipRight, length(UriPart))};
        nomatch ->
            NoMatchResult
    end.

default_port(redis) ->
    6379;
default_port(http) ->
    80;
default_port(https) ->
    443.

int_port(Port) when is_integer(Port) ->
    Port;
int_port(Port) when is_list(Port) ->
    list_to_integer(Port).

path("") ->
    "/";
path(Path) ->
    Path.
