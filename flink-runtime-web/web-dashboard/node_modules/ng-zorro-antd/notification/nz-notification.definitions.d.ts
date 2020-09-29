import { TemplateRef } from '@angular/core';
import { Subject } from 'rxjs';
import { NzMessageData, NzMessageDataOptions } from '../message/nz-message.definitions';
export interface NzNotificationData extends NzMessageData {
    template?: TemplateRef<{}>;
    type?: 'success' | 'info' | 'warning' | 'error' | 'blank' | string;
    title?: string;
}
export interface NzNotificationDataOptions<T = {}> extends NzMessageDataOptions {
    nzKey?: string;
    nzStyle?: any;
    nzClass?: any;
    /** Anything user wants renderer into a template. */
    nzData?: T;
}
export interface NzNotificationDataFilled extends NzNotificationData {
    messageId: string;
    createdAt: Date;
    state?: 'enter' | 'leave';
    options?: NzNotificationDataOptions;
    onClose?: Subject<boolean>;
}
