package shared.messages;

public class Message implements KVMessage {

	private String key;
	private String value;
	private StatusType status;

	public Message(String[] server_response){

		String status = server_response[0];

		if (status.equals("GET_ERROR")) {
			
			this.status = KVMessage.StatusType.GET_ERROR;
			this.key = server_response[2];
			this.value = "";

		} else if (status.equals("GET_SUCCESS")) {

			this.status = KVMessage.StatusType.GET_SUCCESS;
			this.key = server_response[2];
			this.value = findValue(server_response);

		} else if (status.equals("PUT_SUCCESS")) {

			this.status = KVMessage.StatusType.PUT_SUCCESS;
			this.key = server_response[2];
			this.value = findValue(server_response);

		} else if (status.equals("PUT_UPDATE")) {

			this.status = KVMessage.StatusType.PUT_UPDATE;
			this.key = server_response[2];
			this.value = findValue(server_response);

		} else if (status.equals("PUT_ERROR")) {

			this.status = KVMessage.StatusType.PUT_ERROR;
			this.key = server_response[2];
			this.value = findValue(server_response);

		} else if (status.equals("DELETE_SUCCESS")) {

			this.status = KVMessage.StatusType.DELETE_SUCCESS;
			this.key = server_response[2];
			this.value = "";

		} else if (status.equals("DELETE_ERROR")) {

			this.status = KVMessage.StatusType.DELETE_ERROR;
			this.key = server_response[2];
			this.value = "";

		}
	}

	private String findValue(String[] server_response){

		String value = "";
		for (int i =4; i < server_response.length - 2; i++ ){
			value += server_response[i];
			value += " ";
		}
		value += server_response[server_response.length -2];
		return value;
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey(){
		return this.key;
	}

	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue(){
		return this.value;
	}

	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus(){
		return this.status;
	}

}
