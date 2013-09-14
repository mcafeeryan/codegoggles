require 'sinatra'

get '/index' do
	File.read(File.join('public', 'index.html'))
end

get '/' do
	File.read(File.join('public', 'index.html'))
end

get '/post' do
    headers \
            "Access-Control-Allow-Origin"   => "*"
	query = params["sql"]
	content_type :json
	`python pyparse.py \"#{query}\"`
end

configure do
    set :port, 80
end
