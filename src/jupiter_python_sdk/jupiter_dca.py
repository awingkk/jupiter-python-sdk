import json
import time
import struct

import httpx
from httpx._config import Timeout

from typing import Any

from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.system_program import transfer, TransferParams

from solana.rpc.types import TxOpts
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed

from spl.token.instructions import create_associated_token_account, get_associated_token_address, sync_native, SyncNativeParams, close_account, CloseAccountParams
from spl.token.constants import *

from construct import Container
from anchorpy.program.core import Program as AnchorProgram
from anchorpy.program.core import Idl, Provider
from anchorpy.provider import Wallet
from anchorpy import Context
from anchorpy import AccountsCoder


class Jupiter_DCA():
    
    DCA_PROGRAM_ID = Pubkey.from_string("DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M")
    IDL = {"version":"0.1.0","name":"dca","instructions":[{"name":"openDca","accounts":[{"name":"dca","isMut":True,"isSigner":False},{"name":"user","isMut":True,"isSigner":True},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"userAta","isMut":True,"isSigner":False},{"name":"inAta","isMut":True,"isSigner":False},{"name":"outAta","isMut":True,"isSigner":False},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[{"name":"applicationIdx","type":"u64"},{"name":"inAmount","type":"u64"},{"name":"inAmountPerCycle","type":"u64"},{"name":"cycleFrequency","type":"i64"},{"name":"minPrice","type":{"option":"u64"}},{"name":"maxPrice","type":{"option":"u64"}},{"name":"startAt","type":{"option":"i64"}},{"name":"closeWsolInAta","type":{"option":"bool"}}]},{"name":"openDcaV2","accounts":[{"name":"dca","isMut":True,"isSigner":False},{"name":"user","isMut":False,"isSigner":True},{"name":"payer","isMut":True,"isSigner":True},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"userAta","isMut":True,"isSigner":False},{"name":"inAta","isMut":True,"isSigner":False},{"name":"outAta","isMut":True,"isSigner":False},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[{"name":"applicationIdx","type":"u64"},{"name":"inAmount","type":"u64"},{"name":"inAmountPerCycle","type":"u64"},{"name":"cycleFrequency","type":"i64"},{"name":"minPrice","type":{"option":"u64"}},{"name":"maxPrice","type":{"option":"u64"}},{"name":"startAt","type":{"option":"i64"}}]},{"name":"closeDca","accounts":[{"name":"user","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"inAta","isMut":True,"isSigner":False},{"name":"outAta","isMut":True,"isSigner":False},{"name":"userInAta","isMut":True,"isSigner":False},{"name":"userOutAta","isMut":True,"isSigner":False},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[]},{"name":"withdraw","accounts":[{"name":"user","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"dcaAta","isMut":True,"isSigner":False},{"name":"userInAta","isMut":True,"isSigner":False,"isOptional":True},{"name":"userOutAta","isMut":True,"isSigner":False,"isOptional":True},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[{"name":"withdrawParams","type":{"defined":"WithdrawParams"}}]},{"name":"deposit","accounts":[{"name":"user","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inAta","isMut":True,"isSigner":False},{"name":"userInAta","isMut":True,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[{"name":"depositIn","type":"u64"}]},{"name":"withdrawFees","accounts":[{"name":"admin","isMut":True,"isSigner":True},{"name":"mint","isMut":False,"isSigner":False},{"name":"feeAuthority","isMut":False,"isSigner":False,"docs":["CHECK"]},{"name":"programFeeAta","isMut":True,"isSigner":False},{"name":"adminFeeAta","isMut":True,"isSigner":False},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False}],"args":[{"name":"amount","type":"u64"}]},{"name":"initiateFlashFill","accounts":[{"name":"keeper","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inputMint","isMut":False,"isSigner":False,"docs":["The token to borrow"]},{"name":"keeperInAta","isMut":True,"isSigner":False,"docs":["The account to send borrowed tokens to"]},{"name":"inAta","isMut":True,"isSigner":False,"docs":["The account to borrow from"]},{"name":"outAta","isMut":False,"isSigner":False,"docs":["The account to repay to"]},{"name":"instructionsSysvar","isMut":False,"isSigner":False,"docs":["Solana Instructions Sysvar"]},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False}],"args":[]},{"name":"fulfillFlashFill","accounts":[{"name":"keeper","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"keeperInAta","isMut":False,"isSigner":False},{"name":"inAta","isMut":False,"isSigner":False},{"name":"outAta","isMut":False,"isSigner":False},{"name":"feeAuthority","isMut":False,"isSigner":False,"docs":["CHECK"]},{"name":"feeAta","isMut":True,"isSigner":False},{"name":"instructionsSysvar","isMut":False,"isSigner":False,"docs":["Solana Instructions Sysvar"]},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[{"name":"repayAmount","type":"u64"}]},{"name":"transfer","accounts":[{"name":"keeper","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"user","isMut":True,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"dcaOutAta","isMut":True,"isSigner":False},{"name":"userOutAta","isMut":True,"isSigner":False,"isOptional":True},{"name":"intermediateAccount","isMut":True,"isSigner":False,"isOptional":True},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[]},{"name":"endAndClose","accounts":[{"name":"keeper","isMut":True,"isSigner":True},{"name":"dca","isMut":True,"isSigner":False},{"name":"inputMint","isMut":False,"isSigner":False},{"name":"outputMint","isMut":False,"isSigner":False},{"name":"inAta","isMut":True,"isSigner":False},{"name":"outAta","isMut":True,"isSigner":False},{"name":"user","isMut":True,"isSigner":False},{"name":"userOutAta","isMut":True,"isSigner":False,"isOptional":True},{"name":"initUserOutAta","isMut":True,"isSigner":False,"isOptional":True},{"name":"intermediateAccount","isMut":True,"isSigner":False,"isOptional":True},{"name":"systemProgram","isMut":False,"isSigner":False},{"name":"tokenProgram","isMut":False,"isSigner":False},{"name":"associatedTokenProgram","isMut":False,"isSigner":False},{"name":"eventAuthority","isMut":False,"isSigner":False},{"name":"program","isMut":False,"isSigner":False}],"args":[]}],"accounts":[{"name":"Dca","type":{"kind":"struct","fields":[{"name":"user","type":"publicKey"},{"name":"inputMint","type":"publicKey"},{"name":"outputMint","type":"publicKey"},{"name":"idx","type":"u64"},{"name":"nextCycleAt","type":"i64"},{"name":"inDeposited","type":"u64"},{"name":"inWithdrawn","type":"u64"},{"name":"outWithdrawn","type":"u64"},{"name":"inUsed","type":"u64"},{"name":"outReceived","type":"u64"},{"name":"inAmountPerCycle","type":"u64"},{"name":"cycleFrequency","type":"i64"},{"name":"nextCycleAmountLeft","type":"u64"},{"name":"inAccount","type":"publicKey"},{"name":"outAccount","type":"publicKey"},{"name":"minOutAmount","type":"u64"},{"name":"maxOutAmount","type":"u64"},{"name":"keeperInBalanceBeforeBorrow","type":"u64"},{"name":"dcaOutBalanceBeforeSwap","type":"u64"},{"name":"createdAt","type":"i64"},{"name":"bump","type":"u8"}]}}],"types":[{"name":"WithdrawParams","type":{"kind":"struct","fields":[{"name":"withdrawAmount","type":"u64"},{"name":"withdrawal","type":{"defined":"Withdrawal"}}]}},{"name":"Withdrawal","type":{"kind":"enum","variants":[{"name":"In"},{"name":"Out"}]}}],"events":[{"name":"CollectedFee","fields":[{"name":"userKey","type":"publicKey","index":False},{"name":"dcaKey","type":"publicKey","index":False},{"name":"mint","type":"publicKey","index":False},{"name":"amount","type":"u64","index":False}]},{"name":"Filled","fields":[{"name":"userKey","type":"publicKey","index":False},{"name":"dcaKey","type":"publicKey","index":False},{"name":"inputMint","type":"publicKey","index":False},{"name":"outputMint","type":"publicKey","index":False},{"name":"inAmount","type":"u64","index":False},{"name":"outAmount","type":"u64","index":False},{"name":"feeMint","type":"publicKey","index":False},{"name":"fee","type":"u64","index":False}]},{"name":"Opened","fields":[{"name":"userKey","type":"publicKey","index":False},{"name":"dcaKey","type":"publicKey","index":False},{"name":"inDeposited","type":"u64","index":False},{"name":"inputMint","type":"publicKey","index":False},{"name":"outputMint","type":"publicKey","index":False},{"name":"cycleFrequency","type":"i64","index":False},{"name":"inAmountPerCycle","type":"u64","index":False},{"name":"createdAt","type":"i64","index":False}]},{"name":"Closed","fields":[{"name":"userKey","type":"publicKey","index":False},{"name":"dcaKey","type":"publicKey","index":False},{"name":"inDeposited","type":"u64","index":False},{"name":"inputMint","type":"publicKey","index":False},{"name":"outputMint","type":"publicKey","index":False},{"name":"cycleFrequency","type":"i64","index":False},{"name":"inAmountPerCycle","type":"u64","index":False},{"name":"createdAt","type":"i64","index":False},{"name":"totalInWithdrawn","type":"u64","index":False},{"name":"totalOutWithdrawn","type":"u64","index":False},{"name":"unfilledAmount","type":"u64","index":False},{"name":"userClosed","type":"bool","index":False}]},{"name":"Withdraw","fields":[{"name":"dcaKey","type":"publicKey","index":False},{"name":"inAmount","type":"u64","index":False},{"name":"outAmount","type":"u64","index":False},{"name":"userWithdraw","type":"bool","index":False}]},{"name":"Deposit","fields":[{"name":"dcaKey","type":"publicKey","index":False},{"name":"amount","type":"u64","index":False}]}],"errors":[{"code":6000,"name":"InvalidAmount","msg":"Invalid deposit amount"},{"code":6001,"name":"InvalidCycleAmount","msg":"Invalid deposit amount"},{"code":6002,"name":"InvalidPair","msg":"Invalid pair"},{"code":6003,"name":"TooFrequent","msg":"Too frequent DCA cycle"},{"code":6004,"name":"InvalidMinPrice","msg":"Minimum price constraint must be greater than 0"},{"code":6005,"name":"InvalidMaxPrice","msg":"Maximum price constraint must be greater than 0"},{"code":6006,"name":"InAmountInsufficient","msg":"In amount needs to be more than in amount per cycle"},{"code":6007,"name":"Unauthorized","msg":"Wrong user"},{"code":6008,"name":"NoInATA","msg":"inAta not passed in"},{"code":6009,"name":"NoUserInATA","msg":"userInAta not passed in"},{"code":6010,"name":"NoOutATA","msg":"outAta not passed in"},{"code":6011,"name":"NoUserOutATA","msg":"userOutAta not passed in"},{"code":6012,"name":"InsufficientBalanceInProgram","msg":"Trying to withdraw more than available"},{"code":6013,"name":"InvalidDepositAmount","msg":"Deposit should be more than 0"},{"code":6014,"name":"UserInsufficientBalance","msg":"User has insufficient balance"},{"code":6015,"name":"UnauthorizedKeeper","msg":"Unauthorized Keeper"},{"code":6016,"name":"UnrecognizedProgram","msg":"Unrecognized Program"},{"code":6017,"name":"MathErrors","msg":"Calculation errors"},{"code":6018,"name":"KeeperNotTimeToFill","msg":"Not time to fill"},{"code":6019,"name":"OrderFillAmountWrong","msg":"Order amount wrong"},{"code":6020,"name":"SwapOutAmountBelowMinimum","msg":"Out amount below expectations"},{"code":6021,"name":"WrongAdmin","msg":"Wrong admin"},{"code":6022,"name":"MathOverflow","msg":"Overflow in arithmetic operation"},{"code":6023,"name":"AddressMismatch","msg":"Address Mismatch"},{"code":6024,"name":"ProgramMismatch","msg":"Program Mismatch"},{"code":6025,"name":"IncorrectRepaymentAmount","msg":"Incorrect Repayment Amount"},{"code":6026,"name":"CannotBorrowBeforeRepay","msg":"Cannot Borrow Before Repay"},{"code":6027,"name":"NoRepaymentInstructionFound","msg":"No Repayment Found"},{"code":6028,"name":"MissingSwapInstructions","msg":"Missing Swap Instruction"},{"code":6029,"name":"UnexpectedSwapProgram","msg":"Expected Instruction to use Jupiter Swap Program"},{"code":6030,"name":"UnknownInstruction","msg":"Unknown Instruction"},{"code":6031,"name":"MissingRepayInstructions","msg":"Missing Repay Instruction"},{"code":6032,"name":"KeeperShortchanged","msg":"Keeper Shortchanged"},{"code":6033,"name":"WrongSwapOutAccount","msg":"Jup Swap to Wrong Out Account"},{"code":6034,"name":"WrongTransferAmount","msg":"Transfer amount should be exactly account balance"},{"code":6035,"name":"InsufficientBalanceForRent","msg":"Insufficient balance for rent"},{"code":6036,"name":"UnexpectedSolBalance","msg":"Unexpected SOL amount in intermediate account"},{"code":6037,"name":"InsufficientWsolForTransfer","msg":"Too little WSOL to perform transfer"},{"code":6038,"name":"MissedInstruction","msg":"Did not call initiate_flash_fill"},{"code":6039,"name":"WrongProgram","msg":"Did not call this program's initiate_flash_fill"},{"code":6040,"name":"BalanceNotZero","msg":"Can't close account with balance"},{"code":6041,"name":"UnexpectedWSOLLeftover","msg":"Should not have WSOL leftover in DCA out-token account"},{"code":6042,"name":"IntermediateAccountNotSet","msg":"Should pass in a WSOL intermediate account when transferring SOL"},{"code":6043,"name":"UnexpectedSwapInstruction","msg":"Did not call jup swap"}]}
    
    def __init__(
        self,
        async_client: AsyncClient,
        keypair: Keypair
    ):
        self.rpc = async_client
        self.keypair = keypair  
        self.dca_program = AnchorProgram(
            idl=Idl.from_json(json.dumps(self.IDL)),
            program_id=self.DCA_PROGRAM_ID,
            provider=Provider(
                connection=self.rpc,
                wallet=Wallet(payer=self.keypair),
                opts=TxOpts(skip_preflight=True, preflight_commitment=Processed)
            )
        )
    
    async def get_mint_token_program(
        self,
        token_mint: Pubkey,
    ) -> Pubkey:
        """Returns token mint token program from token mint Pubkey.
        
        Args:
            ``token_mint (Pubkey)``: Pubkey of the token mint.
            
        Returns:
            ``Pubkey (Pubkey)``: Mint token program Pubkey.
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> token_mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            >>> token_mint_program = await jupiter.dca.get_mint_token_program(token_mint)
            
            Pubkey(
                TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA,
            )
        """
        
        mint_token_program_info = await self.rpc.get_account_info(token_mint)
        mint_token_program = mint_token_program_info.value.owner
        return mint_token_program
        
    async def get_or_create_associated_token_address(
        self,
        token_mint: Pubkey,
    ) -> dict:
        """Returns assosciated token address Pubkey with instruction to create it if it doesn't exists.
        
        Args:
            ``token_mint (Pubkey)``: Pubkey of the token mint.
            
        Returns:
            ``Dict``: Associated token address Pubkey and instruction.
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> token_mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            >>> associated_token_address = await jupiter.dca.get_or_create_associated_token_address(token_mint)
            
            {'pubkey': Pubkey(
                2GWpKNsfBq7y2LS9FNpAUWsr6atnywB7JTwRrifMHzwU,
            ), 'instruction': Instruction(
                Instruction {
                    program_id: ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL,
                    accounts: [
                        AccountMeta {
                            pubkey: AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX,
                            is_signer: True,
                            is_writable: True,
                        },
                        AccountMeta {
                            pubkey: 2GWpKNsfBq7y2LS9FNpAUWsr6atnywB7JTwRrifMHzwU,
                            is_signer: False,
                            is_writable: True,
                        },
                        AccountMeta {
                            pubkey: AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX,
                            is_signer: False,
                            is_writable: False,
                        },
                        AccountMeta {
                            pubkey: EPjFWdd1ufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v,
                            is_signer: False,
                            is_writable: False,
                        },
                        AccountMeta {
                            pubkey: 11111111111111111111111111111111,
                            is_signer: False,
                            is_writable: False,
                        },
                        AccountMeta {
                            pubkey: TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA,
                            is_signer: False,
                            is_writable: False,
                        },
                        AccountMeta {
                            pubkey: SysvarRent111111111111111111111111111111111,
                            is_signer: False,
                            is_writable: False,
                        },
                    ],
                    data: [],
                },
            )}
        """
        
        toAccount = get_associated_token_address(self.keypair.pubkey(), token_mint)
        accountInfo = await self.rpc.get_account_info(toAccount)
        account = accountInfo.value
        
        if account:
            instruction = None
            return {'pubkey': toAccount, 'instruction': instruction}
        else:
            instruction = create_associated_token_account(self.keypair.pubkey(), self.keypair.pubkey(), token_mint)
            return {'pubkey': toAccount, 'instruction': instruction}
    
    async def get_dca_pubkey(
        self,
        input_mint: Pubkey,
        output_mint: Pubkey,
        uid: int,
    ) -> Pubkey:
        """Returns DCA Pubkey to be created.
        
        Args:
            ``input_mint (Pubkey)``: Pubkey of the token to sell.
            ``output_mint (Pubkey)``: Pubkey of the token to buy.
            ``uid (int)``: A unix timestamp in seconds.
            
        Returns:
            ``Pubkey``: DCA Pubkey to be created.
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> input_mint = Pubkey.from_string("So11111111111111111111111111111111111111112")
            >>> output_mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            >>> uid = int(time.time())
            >>> dca_pubkey = await jupiter.dca.get_dca_pubkey(input_mint, output_mint, uid)
            
            Pubkey(
                ArotVdXCEw4Zy5ywUVP7ZJBPFvXR8v8Md6jSQK9pLaCb,
            )
        """ 
            
        dcaPubKey = Pubkey.find_program_address(
            seeds=[
            b'dca',
            self.keypair.pubkey().__bytes__(),
            input_mint.__bytes__(),
            output_mint.__bytes__(),
            struct.pack("<Q", uid)
            ],
            program_id=self.DCA_PROGRAM_ID
        )
        return dcaPubKey[0]

    async def fetch_dca_data(
        self,
        dca_pubkey: Pubkey,
    ) -> Container[Any]:
        """Fetch DCA Account Anchor Data
        
        Args:
            ``dca_pubkey (Pubkey)``: Pubkey of the DCA account.
        
        Returns:
            ``Dca``: DCA Account Anchor Data
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> dca_pubkey = Pubkey.from_string("45iYdjmFUHSJCQHnNpWNFF9AjvzRcsQUP9FDBvJCiNS1")
            >>> fetch_dca_data = await jupiter.dca.fetch_dca_data(dca_pubkey)
            Dca(user=Pubkey(
                AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX,
            ), input_mint=Pubkey(
                So11111111111111111111111111111111111111112,
            ), output_mint=Pubkey(
                EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v,
            ), idx=1703051020, next_cycle_at=1703052580, in_deposited=5000000, in_withdrawn=0, out_withdrawn=194361, in_used=2600000, out_received=194361, in_amount_per_cycle=100000, cycle_frequency=60, next_cycle_amount_left=100000, in_account=Pubkey(
                3NFmVtdyxQvmhXa1a82j62dd9kCmKFa3uv3vv7WRQsRY,
            ), out_account=Pubkey(
                ErFYjASdQ5HVtJoQ3dqrjp8MgFKjSjFVQiRSqBbKT9kJ,
            ), min_out_amount=0, max_out_amount=0, keeper_in_balance_before_borrow=0, dca_out_balance_before_swap=0, created_at=1703050997, bump=253)
        """ 
        dca_account_coder = AccountsCoder(idl=Idl.from_json(json.dumps(self.IDL)))
        get_dca_account_details  = await self.rpc.get_account_info(dca_pubkey)
        dca_account_details = get_dca_account_details.value.data
        dca_account_decoded_details = dca_account_coder.decode(dca_account_details)
        return dca_account_decoded_details

    async def create_dca(
        self,
        input_mint: Pubkey,
        output_mint: Pubkey,
        total_in_amount: int,
        in_amount_per_cycle: int,
        cycle_frequency: int,
        min_out_amount_per_cycle: int=None,
        max_out_amount_per_cycle: int=None,
        start_at: int=0,
        ) -> dict:
        """Setting up a DCA account with signing and sending the transaction.
        
        Args:
            ``input_mint (Pubkey)``: Pubkey of the token to sell.
            ``output_mint (Pubkey)``: Pubkey of the token to buy.
            ``total_in_amount (int)``: Total input mint amount to sell.
            ``in_amount_per_cycle (int)``: Input mint amount to sell each time. For e.g. if you are trying to buy SOL using 100 USDC every day over 10 days, in_amount_per_cycle should be 100*10**6.
            ``cycle_frequency (int)``: The number of seconds between each periodic buys. For e.g. if you are trying to DCA on a daily basis, cycle_frequency should be 60*60*24 = 86,400.
            ``min_out_amount_per_cycle (int)``: Optional field. Following the examples above, let's say you only want to buy SOL if SOL is below SOL-USDC $20, that means for each cycle, with every 100 USDC, you want to receive a minimum of 100 / 20 = 5 SOL. You can then pass 5 * LAMPORTS_PER_SOL as argument here. This ensures that you receive > 5 SOL for each order.
            ``max_out_amount_per_cycle (int)``: This is just the inverse scenario of min_out_amount_per_cycle. While max_out_amount_per_cycle is a little counter intuitive, it can be used by advanced traders / investors who believe an asset is at risk of further decline in prices if it goes beyond a certain threshold. Say in the case of output mint being a stablecoin, if the stablecoin drops to $0.5, you will get more buying into it, but that may not necessary be a good thing since the risk of a stablecoin going to $0 is very real if it could depeg to $0.5. This is where max_out_amount_per_cycle could be useful.
            ``start_at (int)``: Optional field. Unix timestamp in seconds of when you would like DCA to start. Pass 0 if you want to start immediately or pass a future time as a unix timestamp in seconds.
            
        Returns:
            ``Dict``: Transaction hash and DCA Account Pubkey.
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> input_mint = Pubkey.from_string("So11111111111111111111111111111111111111112")
            >>> output_mint = Pubkey.from_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            >>> total_in_amount = 5_000_000
            >>> in_amount_per_cycle = 100
            >>> cycle_frequency = 60
            >>> min_out_amount_per_cycle = 0
            >>> max_out_amount_per_cycle = 0
            >>> start = 0
            >>> create_dca = await jupiter.dca.create_dca(input_mint, output_mint, total_in_amount, in_amount_per_cycle, cycle_frequency, min_out_amount_per_cycle, max_out_amount_per_cycle, start)

            {
                'dca_pubkey': Pubkey(Duop8ynu7WozpmLUsc1pAq6gobS3vdxe8GJMFzF8rY56,),
                'transaction_hash': '5zFwgyHExYcWLTgqh6GMDPML2mPHzmJUFfsVD882tYd4oGhekiDufcBZSbcoNFKxvhRLQt8WwYU4SsW8fCefYmHf'
            }
        """
        
        pre_instructions = []
        post_instructions = []
    
        input_token_program, output_token_program = await self.get_mint_token_program(input_mint), await self.get_mint_token_program(output_mint)
        user_input_account = get_associated_token_address(self.keypair.pubkey(), input_mint)
        
        if input_mint == WRAPPED_SOL_MINT:
            input_mint_ata = await self.get_or_create_associated_token_address(input_mint)
            transfer_IX = transfer(TransferParams(
                from_pubkey=self.keypair.pubkey(),
                to_pubkey=user_input_account,
                lamports=total_in_amount
            ))
            sync_native_IX = sync_native(SyncNativeParams(
                program_id=input_token_program,
                account=user_input_account
            ))
            
            if input_mint_ata['instruction']:
                pre_instructions.append(input_mint_ata['instruction'])
                post_instructions.append(close_account(
                    CloseAccountParams(
                        program_id=input_mint_ata['pubkey'],
                        account=self.keypair.pubkey(),
                        dest=self.keypair.pubkey(),
                        owner=self.keypair.pubkey()
                    )
                ))
                
            pre_instructions.append(transfer_IX)
            pre_instructions.append(sync_native_IX)
        
        if output_mint != WRAPPED_SOL_MINT:
            output_mint_ata = await self.get_or_create_associated_token_address(output_mint)
            
            if output_mint_ata['instruction']:
                pre_instructions.append(output_mint_ata['instruction'])
        
        uid = int(time.time())
        dca_pubkey = await self.get_dca_pubkey(input_mint, output_mint, uid)

        accounts = {
            'dca': dca_pubkey,
            'user': self.keypair.pubkey(),
            'payer': self.keypair.pubkey(),
            'input_mint': input_mint,
            'output_mint': output_mint,
            'user_ata': user_input_account,
            'in_ata': get_associated_token_address(dca_pubkey, input_mint),
            'out_ata': get_associated_token_address(dca_pubkey, output_mint),
            'system_program': Pubkey.from_string("11111111111111111111111111111111"),
            'token_program': Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
            'associated_token_program': Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"),
            'event_authority': Pubkey.from_string("Cspp27eGUDMXxPEdhmEXFVRn6Lt1L7xJyALF3nmnWoBj"),
            'program': self.DCA_PROGRAM_ID 
        }

        transaction = await self.dca_program.rpc['open_dca'](
            uid,
            total_in_amount,
            in_amount_per_cycle,
            cycle_frequency,
            min_out_amount_per_cycle,
            max_out_amount_per_cycle,
            start_at,
            False,
            ctx=Context(
                accounts=accounts,
                signers=[self.keypair],
                pre_instructions=pre_instructions,
                # post_instructions=post_instructions,
                options=TxOpts(skip_preflight=False, preflight_commitment=Processed)
            )
        )

        return {'dca_pubkey': dca_pubkey, 'transaction_hash': str(transaction)}

    async def close_dca(
        self,
        dca_pubkey: Pubkey,
    ) -> str:
        """Close DCA Account with signing and sending the transaction.
        
        Args:
            ``dca_pubkey (Pubkey)``: Pubkey of the DCA account to be closed.
        
        Returns:
            ``str``: transaction hash of the DCA account closed.
        
        Example:
            >>> rpc_url = "https://neat-hidden-sanctuary.solana-mainnet.discover.quiknode.pro/2af5315d336f9ae920028bbb90a73b724dc1bbed/"
            >>> async_client = AsyncClient(rpc_url)
            >>> private_key_string = "tSg8j3pWQyx3TC2fpN9Ud1bS0NoAK0Pa3TC2fpNd1bS0NoASg83TC2fpN9Ud1bS0NoAK0P"
            >>> private_key = Keypair.from_bytes(base58.b58decode(private_key_string))
            >>> jupiter = Jupiter(async_client, private_key)
            >>> dca_pubkey = Pubkey.from_string("45iYdjmFUHSJCQHnNpWNFF9AjvzRcsQUP9FDBvJCiNS1")
            >>> close_dca = await jupiter.dca.close_dca(dca_pubkey)
            HXiWtTPLjgtiuNoAN7CEfyDyftdsAnMfuQ93Yp1vukgBbdU1Lb2Bo59p48PVr7CMhxPDwGQbsfjvKT5HXXQfvE2
        """
        dca_account = await self.fetch_dca_data(dca_pubkey)
        input_mint = dca_account.input_mint
        output_mint = dca_account.output_mint
        input_token_program, output_token_program = await self.get_mint_token_program(input_mint), await self.get_mint_token_program(output_mint)
        user_input_account = get_associated_token_address(self.keypair.pubkey(), input_mint)
        user_output_account = get_associated_token_address(self.keypair.pubkey(), output_mint)
        reserve_input_account = get_associated_token_address(dca_pubkey, input_mint)
        reserve_output_account = get_associated_token_address(dca_pubkey, output_mint)
        
        accounts = {
            'user': self.keypair.pubkey(),
            'dca': dca_pubkey,
            'input_mint': input_mint,
            'output_mint': output_mint,
            'in_ata': reserve_input_account,
            'out_ata': reserve_output_account,
            'user_in_ata': user_input_account,
            'user_out_ata': user_output_account,
            'system_program': Pubkey.from_string("11111111111111111111111111111111"),
            'token_program': Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
            'associated_token_program': Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"),
            'event_authority': Pubkey.from_string("Cspp27eGUDMXxPEdhmEXFVRn6Lt1L7xJyALF3nmnWoBj"),
            'program': self.DCA_PROGRAM_ID 
        }
        transaction = await self.dca_program.rpc['close_dca'](
            ctx=Context(
                accounts=accounts,
                signers=[self.keypair],
                # pre_instructions=pre_instructions,
                # post_instructions=post_instructions,
                options=TxOpts(skip_preflight=False, preflight_commitment=Processed)
            )
        )
        return str(transaction)

    @staticmethod
    async def fetch_user_dca_accounts(
        wallet_address: str,
        status: int
    ) -> dict:
        """Returns all DCA Accounts from a wallet address

        Args:
            ``wallet_address (str)``: Wallet address.
            ``status (int)``: 0 = DCA Running | 1 = DCA Done
                
        Returns:
            ``dict``: all DCA Accounts from the wallet address.

        Example:
            >>> wallet_address = "AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX"
            >>> status = 0
            >>> user_dca_accounts = await Jupiter_DCA.fetch_user_dca_accounts(wallet_address, status)
        {
            'ok': True,
            'data': {
                'dcaAccounts': [
                    {
                        'id': 202513,
                        'createdAt': '2023-12-18T12:39:40.000Z',
                        'updatedAt': '2023-12-18T12:39:42.265Z',
                        'userKey': 'AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX',
                        'dcaKey': 'FTG9eN99uJJBqBtcz1bR6pGfj2Pb3yNAZmGYXggcAgio',
                        'inputMint': 'So11111111111111111111111111111111111111112',
                        'outputMint': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                        'inDeposited': '1000000',
                        'inAmountPerCycle': '100',
                        'cycleFrequency': '60',
                        'closeTxHash': '',
                        'openTxHash': '2bwo4ds3Xpiwr9gLptXVFTcxnz2rP9pu3fAppNn3E61g33tprBCy6X8PT3SaXMn7fpG2c5CRw1LWGMEz3ik5SgnP',
                        'status': 0,
                        'inWithdrawn': '0',
                        'outWithdrawn': '0',
                        'unfilledAmount': '1000000',
                        'userClosed': False, 
                        'fills': []
                    }
                ]
            }
        }
        """
        user_dca_accounts = httpx.get(f"https://dca-api.jup.ag/user/{wallet_address}/dca?status={status}", timeout=Timeout(timeout=30.0)).json()
        return user_dca_accounts
    
    @staticmethod
    async def fetch_dca_account_fills_history(
        dca_account_address: str,
    ) -> dict:
        """Returns all DCA Account fills history

        Args:
            ``dca_account_address (str)``: DCA Account address.
                
        Returns:
            ``dict``: all DCA Account fills history.

        Example:
            >>> dca_account_address = "C91FGJAvQgeaXka1exMihC5qChwdcJzFemFVQutv4dev"
            >>> dca_account_fills_history = await Jupiter_DCA.fetch_dca_fills_history(dca_account_address)
        {
            'ok': True,
            'data': {
                'fills': [
                    {
                        'userKey': 'AyWu89SjZBW1MzkxiREmgtyMKxSkS1zVy8Uo23RyLphX',
                        'dcaKey': 'C91FGJAvQgeaXka1exMihC5qChwdcJzFemFVQutv4dev',
                        'inputMint': 'So11111111111111111111111111111111111111112', 
                        'outputMint': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                        'inAmount': '100000',
                        'outAmount': '7540',
                        'feeMint': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
                        'fee': '7',
                        'txId': '48kC4ZnkPpiRc6T3eVKYghmUERa2geC2e9FvGbVjDMaNkbrDJ3tj36GMkNdi1AkXkqrAHoDZcLW5SFjQEZcKB2NB',
                        'confirmedAt': 1703055843
                    }
                ]
            }
        }    
        """
        dca_account_fills_history = httpx.get(f"https://dca-api.jup.ag/dca/{dca_account_address}/fills", timeout=Timeout(timeout=30.0)).json()
        return dca_account_fills_history
          
    @staticmethod
    async def get_available_dca_tokens(
    ) -> list:
        """Get available tokens for DCA.
        
        Returns:
            ``list``: all available tokens from https://cache.jup.ag/top-tokens
        
        Example:
            >>> available_dca_tokens = await Jupiter_DCA.get_available_dca_tokens()
        """
        available_dca_tokens = httpx.get(f"https://cache.jup.ag/top-tokens", timeout=Timeout(timeout=30.0)).json()
        return available_dca_tokens
    