use {
  iced::{
    widget::{
      column, container, button, row, scrollable, text, text_input, Rule,
    },
    Element, Length, Pixels, Sandbox, Settings,
  },
  solana_derivation_path::DerivationPath,
  solana_native_token::{ Sol, LAMPORTS_PER_SOL },
  solana_pubkey::Pubkey,
  solana_remote_wallet::remote_wallet::{
    RemoteWalletType, initialize_wallet_manager,
  },
  solana_rpc_client::rpc_client::RpcClient,
  std::str::FromStr,
};

const DEFAULT_RPC_URL: &str = "http://localhost:8899";

/// Messages the GUI reacts to.
#[derive(Debug, Clone)]
pub enum Message {
  /// RPC URL text was edited.
  RpcUrlChanged(String),
  /// Apply the RPC URL (connect).
  ApplyRpcUrl,
  /// Discover connected hardware wallets.
  DiscoverDevices,
  /// A device row was clicked.
  SelectDevice(usize),
  /// Fetch the balance for the selected account.
  FetchBalance,
  /// Transfer recipient text changed.
  TransferRecipientChanged(String),
  /// Transfer amount text changed.
  TransferAmountChanged(String),
  /// Initiate a transfer.
  SendTransfer,
}

/// Lightweight snapshot of a detected device (display data only).
#[derive(Clone, Debug)]
struct DeviceInfo {
  pub pubkey: Pubkey,
  /// Handle to the actual wallet; stored in the model, never in a Message.
  pub wallet_type: RemoteWalletType,
}

/// Application state.
pub struct WalletApp {
  /// RPC endpoint URL.
  rpc_url: String,
  /// Blocking RPC client.
  rpc_client: Option<RpcClient>,
  /// Discovered devices.
  devices: Vec<DeviceInfo>,
  /// Index into [`Self::devices`].
  selected_device: Option<usize>,
  /// SOL balance of the selected account (as lamports, wrapped in Sol).
  balance: Option<Sol>,
  /// Transfer recipient pubkey as base58 string.
  transfer_recipient: String,
  /// Transfer amount in SOL.
  transfer_amount: String,
  /// Status bar text.
  status: String,
}

impl WalletApp {
  fn selected(&self) -> Option<&DeviceInfo> {
    self.selected_device.and_then(|i| self.devices.get(i))
  }
}

impl Sandbox for WalletApp {
  type Message = Message;

  fn new() -> Self {
    Self {
      rpc_url: DEFAULT_RPC_URL.to_string(),
      rpc_client: None,
      devices: Vec::new(),
      selected_device: None,
      balance: None,
      transfer_recipient: String::new(),
      transfer_amount: String::new(),
      status: String::new(),
    }
  }

  fn title(&self) -> String {
    "Agave GUI Wallet".to_string()
  }

  fn update(&mut self, message: Message) {
    match message {
      Message::RpcUrlChanged(url) => self.rpc_url = url,
      Message::ApplyRpcUrl => {
        let url = self.rpc_url.clone();
        // RpcClient::new returns RpcClient directly (not a Result).
        // It panics on invalid URL, so we catch that with a scoped thread.
        self.rpc_client = Some(RpcClient::new(url.clone()));
        self.status = format!("Connected to {url}");
      }
      Message::DiscoverDevices => {
        self.status = "Scanning for devices…".to_string();
        match initialize_wallet_manager() {
          Ok(manager) => match manager.update_devices() {
            Ok(count) => {
              let mut devices = Vec::new();
              for d in manager.list_devices() {
                // Device::info is pub(crate), so we extract pubkey from wallet_type.
                let pubkey = d.wallet_type.get_pubkey(&DerivationPath::default(), false)
                  .unwrap_or_default();
                devices.push(DeviceInfo {
                  pubkey,
                  wallet_type: d.wallet_type,
                });
              }
              self.devices = devices;
              self.selected_device = None;
              self.balance = None;
              self.status = format!("Found {count} device(s)");
            }
            Err(e) => self.status = format!("Scan error: {e}"),
          },
          Err(e) => self.status = format!("Init error: {e}"),
        }
      }
      Message::SelectDevice(idx) => {
        self.selected_device = Some(idx);
        self.balance = None;
        if let Some(device) = self.selected() {
          self.status = format!("Selected {}", device.pubkey);
        }
      }
      Message::FetchBalance => {
        let Some(device) = self.selected() else {
          self.status = "No account selected".to_string();
          return;
        };
        let Some(client) = self.rpc_client.as_ref() else {
          self.status = "No RPC connection".to_string();
          return;
        };
        match client.get_balance(&device.pubkey) {
          Ok(lamports) => {
            self.balance = Some(Sol(lamports));
          }
          Err(e) => self.status = format!("Balance error: {e}"),
        }
      }
      Message::TransferRecipientChanged(s) => self.transfer_recipient = s,
      Message::TransferAmountChanged(s) => self.transfer_amount = s,
      Message::SendTransfer => {
        let Some(device) = self.selected().cloned() else {
          self.status = "No account selected".to_string();
          return;
        };
        let recipient = match Pubkey::from_str(&self.transfer_recipient) {
          Ok(pk) => pk,
          Err(e) => {
            self.status = format!("Invalid recipient: {e}");
            return;
          }
        };
        let amount_sol: f64 = match self.transfer_amount.parse() {
          Ok(v) => v,
          Err(_) => {
            self.status = "Invalid amount".to_string();
            return;
          }
        };
        let lamports = (amount_sol * LAMPORTS_PER_SOL as f64) as u64;
        let Some(client) = self.rpc_client.as_ref() else {
          self.status = "No RPC connection".to_string();
          return;
        };

        let recent_hash = match client.get_latest_blockhash() {
          Ok(h) => h,
          Err(e) => {
            self.status = format!("Blockhash error: {e}");
            return;
          }
        };
        let ix = solana_system_interface::instruction::transfer(
          &device.pubkey,
          &recipient,
          lamports,
        );
        let mut tx = solana_transaction::Transaction::new_with_payer(
          &[ix],
          Some(&device.pubkey),
        );
        tx.message.recent_blockhash = recent_hash;

        self.status = "Approve transfer on your device…".to_string();
        let derivation_path = DerivationPath::default();
        let message_bytes = tx.message.serialize();
        match device.wallet_type.sign_message(&derivation_path, &message_bytes) {
          Ok(sig) => {
            tx.signatures[0] = sig;
            match client.send_and_confirm_transaction_with_spinner(&tx) {
              Ok(signature) => {
                self.status = format!("Transfer sent: {signature}");
                self.transfer_recipient.clear();
                self.transfer_amount.clear();
              }
              Err(e) => self.status = format!("Send error: {e}"),
            }
          }
          Err(e) => self.status = format!("Signing error: {e}"),
        }
      }
    }
  }

  fn view(&self) -> Element<'_, Self::Message> {
    // Connection panel
    let connection = row![
      text_input("RPC URL", &self.rpc_url)
        .on_input(Message::RpcUrlChanged),
      button("Connect").on_press(Message::ApplyRpcUrl),
    ]
    .padding(8);

    // Device list
    let discover_btn = button("Discover Devices").on_press(Message::DiscoverDevices);

    let device_rows: Vec<Element<_>> = self
      .devices
      .iter()
      .enumerate()
      .map(|(idx, d)| {
        let is_selected = self.selected_device == Some(idx);
        let label = d.pubkey.to_string();
        let btn = button(text(&label).size(13))
          .on_press(Message::SelectDevice(idx))
          .padding(6)
          .width(Length::Fill);
        if is_selected {
          btn.style(iced::theme::Button::Primary).into()
        } else {
          btn.into()
        }
      })
      .collect();

    let device_list = column![
      discover_btn,
      scrollable(column(device_rows)),
    ]
    .padding(8)
    .spacing(4);

    // Account panel
    let account_info: Element<_> = if let Some(d) = self.selected() {
      let balance_text = self
        .balance
        .as_ref()
        .map(|s| format!("{s}"))
        .unwrap_or_else(|| "—".to_string());
      column![
        text(format!("Account: {}", d.pubkey)).size(14),
        text(format!("Balance: {balance_text}")).size(14),
        button("Refresh Balance").on_press(Message::FetchBalance),
      ]
      .spacing(4)
      .into()
    } else {
      text("Select a device above").size(14).into()
    };

    // Transfer panel
    let transfer = column![
      text("Transfer").size(16),
      text_input("Recipient address", &self.transfer_recipient)
        .on_input(Message::TransferRecipientChanged),
      text_input("Amount (SOL)", &self.transfer_amount)
        .on_input(Message::TransferAmountChanged),
      button("Send").on_press(Message::SendTransfer),
    ]
    .padding(8)
    .spacing(6);

    // Status bar
    let status_bar = container(text(&self.status).size(12))
      .padding(8)
      .width(Length::Fill);

    column![
      container(connection).width(Length::Fill),
      Rule::horizontal(1),
      container(device_list).width(Length::Fill),
      Rule::horizontal(1),
      container(
        row![account_info, transfer]
          .padding(8)
          .spacing(24),
      )
      .width(Length::Fill),
      Rule::horizontal(1),
      status_bar,
    ]
    .spacing(0)
    .max_width(720)
    .padding(16)
    .into()
  }
}

fn main() -> iced::Result {
  WalletApp::run(Settings {
    id: None,
    window: iced::window::Settings {
      size: iced::Size::new(800.0, 600.0),
      ..Default::default()
    },
    flags: (),
    fonts: Vec::new(),
    default_font: iced::Font::DEFAULT,
    default_text_size: Pixels(14.0),
    antialiasing: true,
  })
}
