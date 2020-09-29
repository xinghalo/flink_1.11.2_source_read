import { ChangeDetectorRef, OnDestroy, OnInit } from '@angular/core';
import { NzMessageContainerComponent } from './nz-message-container.component';
import { NzMessageDataFilled, NzMessageDataOptions } from './nz-message.definitions';
export declare class NzMessageComponent implements OnInit, OnDestroy {
    private _messageContainer;
    protected cdr: ChangeDetectorRef;
    nzMessage: NzMessageDataFilled;
    nzIndex: number;
    protected _options: Required<NzMessageDataOptions>;
    private _autoErase;
    private _eraseTimer;
    private _eraseTimingStart;
    private _eraseTTL;
    constructor(_messageContainer: NzMessageContainerComponent, cdr: ChangeDetectorRef);
    ngOnInit(): void;
    ngOnDestroy(): void;
    onEnter(): void;
    onLeave(): void;
    protected _destroy(userAction?: boolean): void;
    private _initErase;
    private _updateTTL;
    private _startEraseTimeout;
    private _clearEraseTimeout;
}
