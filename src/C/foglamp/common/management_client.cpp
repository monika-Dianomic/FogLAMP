/*
 * FogLAMP storage service.
 *
 * Copyright (c) 2017 OSisoft, LLC
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <management_client.h>
#include <rapidjson/document.h>
#include <service_record.h>
#include <string>
#include <sstream>
#include <iostream>

using namespace std;
using namespace rapidjson;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;

/**
 * Management Client constructor
 */
ManagementClient::ManagementClient(const string& hostname, const short port)
{
ostringstream urlbase;

	m_logger = Logger::getLogger();
	urlbase << hostname << ":" << port;
	m_client = new HttpClient(urlbase.str());
}

/**
 * Destructor for management client
 */
ManagementClient::~ManagementClient()
{
	delete m_client;
}

/**
 * Register this service
 */
bool ManagementClient::registerService(const ServiceRecord& service)
{
string payload;

	try {
		service.asJSON(payload);
		auto res = m_client->request("POST", "/foglamp/service", payload);
		Document doc;
		doc.Parse(res->content.string().c_str());
		if (doc.HasParseError())
		{
			m_logger->error("Failed to parse result of registration: %s\n",
				res->content.string().c_str());
			return false;
		}
		if (doc.HasMember("id"))
		{
			m_uuid = new string(doc["id"].GetString());
			m_logger->info("Registered service %s.\n", m_uuid->c_str());
			return true;
		}
		else if (doc.HasMember("message"))
		{
			m_logger->error("Failed to register service: %s.",
				doc["message"].GetString());
		}
	} catch (const SimpleWeb::system_error &e) {
		m_logger->error("Register service failed %s.", e.what());
		return false;
	}
	return false;
}

/**
 * Unregister this service
 */
bool ManagementClient::unregisterService()
{

	try {
		string url = "/foglamp/service/";
		url += *m_uuid;
		auto res = m_client->request("DELETE", url.c_str());
		Document doc;
		doc.Parse(res->content.string().c_str());
		if (doc.HasParseError())
		{
			m_logger->error("Failed to parse result of unregistration: %s\n",
				res->content.string().c_str());
			return false;
		}
		if (doc.HasMember("id"))
		{
			m_uuid = new string(doc["id"].GetString());
			m_logger->info("Unregistered service %s.\n", m_uuid->c_str());
			return true;
		}
		else if (doc.HasMember("message"))
		{
			m_logger->error("Failed to unregister service: %s.",
				doc["message"].GetString());
		}
	} catch (const SimpleWeb::system_error &e) {
		m_logger->error("Unregister service failed %s.", e.what());
		return false;
	}
	return false;
}

/**
 * Register interest in a configuration category
 */
bool ManagementClient::registerCategory(const string& category)
{
ostringstream convert;

	try {
		convert << "{ \"category\" : \"" << category << "\", ";
		convert << "\"service\" : \"" << m_uuid << "\" }";
		auto res = m_client->request("POST", "/foglamp/interest", convert.str());
		Document doc;
		doc.Parse(res->content.string().c_str());
		if (doc.HasParseError())
		{
			m_logger->error("Failed to parse result of category registration: %s\n",
				res->content.string().c_str());
			return false;
		}
		if (doc.HasMember("id"))
		{
			const char *reg_id = doc["id"].GetString();
			m_categories[category] = string(reg_id);
			return true;
		}
		else if (doc.HasMember("message"))
		{
			m_logger->error("Failed to register service: %s.",
				doc["message"].GetString());
		}
	} catch (const SimpleWeb::system_error &e) {
                m_logger->error("Register configuration category failed %s.", e.what());
                return false;
        }
        return false;
}

/**
 * Unegister interest in a configuration category
 */             
bool ManagementClient::unregisterCategory(const string& category)
{               
ostringstream convert;
        
        try {   
		string url = "/foglamp/interest/";
		url += m_categories[category];
                auto res = m_client->request("DELETE", url.c_str());
        } catch (const SimpleWeb::system_error &e) {
                m_logger->error("Unregister configuration category failed %s.", e.what());
                return false;
        }
        return false;
}
