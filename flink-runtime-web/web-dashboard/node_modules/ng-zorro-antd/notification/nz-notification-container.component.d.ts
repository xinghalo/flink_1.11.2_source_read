import { ChangeDetectorRef } from '@angular/core';
import { NzMessageContainerComponent } from '../message/nz-message-container.component';
import { NzNotificationConfig } from './nz-notification-config';
import { NzNotificationDataFilled } from './nz-notification.definitions';
export declare class NzNotificationContainerComponent extends NzMessageContainerComponent {
    config: Required<NzNotificationConfig>;
    bottom: string | null;
    /**
     * @override
     */
    messages: Array<Required<NzNotificationDataFilled>>;
    constructor(cdr: ChangeDetectorRef, defaultConfig: NzNotificationConfig, config: NzNotificationConfig);
    /**
     * @override
     */
    setConfig(config: NzNotificationConfig): void;
    /**
     * Create a new notification.
     * If there's a notification whose `nzKey` is same with `nzKey` in `NzNotificationDataFilled`,
     * replace its content instead of create a new one.
     * @override
     * @param notification
     */
    createMessage(notification: NzNotificationDataFilled): void;
    private replaceNotification;
}
